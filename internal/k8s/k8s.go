// Package k8s wraps client-go for dynamically starting/stopping worker pods.
package k8s

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	workerPort = 48080

	// labelOwned marks objects created by this service (vs. Helm/system
	// deployments), so ListDeployments only returns what ops-extension owns.
	labelOwned = "zergx/owned"
	// labelSession ties a sandbox pod or deployment to a session key (the
	// deterministic hash of the raw "org:repo:bookmark" name).
	labelSession = "zergx/session"
	// annSession preserves the raw session name for display.
	annSession = "zergx/session"
)

// Config mirrors K8sConfig in the original executor.
type Config struct {
	Namespace   string
	WorkerImage string
	// Resource requests/limits for worker containers AND deployments.
	// Namespaces with a ResourceQuota (like temp) reject pods without them.
	CPURequest    string
	CPULimit      string
	MemoryRequest string
	MemoryLimit   string
}

// resourcesFor returns the configured resource requirements (quota-safe
// defaults when unset).
func (m *Manager) resourcesFor() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(orDefault(m.config.CPURequest, "250m")),
			corev1.ResourceMemory: resource.MustParse(orDefault(m.config.MemoryRequest, "256Mi")),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(orDefault(m.config.CPULimit, "1")),
			corev1.ResourceMemory: resource.MustParse(orDefault(m.config.MemoryLimit, "1Gi")),
		},
	}
}

// ResourcePair is one side of a nested resource declaration.
type ResourcePair struct {
	CPU    string `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
}

// ResourceRequest is the nested `resources:{requests:{...},limits:{...}}`
// shape accepted by the deploy API and the container-deploy tool. Empty means
// "fall back to the manager's global defaults".
type ResourceRequest struct {
	Requests *ResourcePair `json:"requests,omitempty"`
	Limits   *ResourcePair `json:"limits,omitempty"`
}

// Requirements converts a nested resource request into a
// *corev1.ResourceRequirements, or nil when nothing was specified. Quantities
// are parsed (never MustParse) so a malformed value is a caller error, not a
// panic.
func (r *ResourceRequest) Requirements() (*corev1.ResourceRequirements, error) {
	if r == nil || (r.Requests == nil && r.Limits == nil) {
		return nil, nil
	}
	out := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{},
		Limits:   corev1.ResourceList{},
	}
	fill := func(list corev1.ResourceList, pair *ResourcePair, side string) error {
		if pair == nil {
			return nil
		}
		if pair.CPU != "" {
			q, err := resource.ParseQuantity(pair.CPU)
			if err != nil {
				return fmt.Errorf("resources.%s.cpu: %w", side, err)
			}
			list[corev1.ResourceCPU] = q
		}
		if pair.Memory != "" {
			q, err := resource.ParseQuantity(pair.Memory)
			if err != nil {
				return fmt.Errorf("resources.%s.memory: %w", side, err)
			}
			list[corev1.ResourceMemory] = q
		}
		return nil
	}
	if err := fill(out.Requests, r.Requests, "requests"); err != nil {
		return nil, err
	}
	if err := fill(out.Limits, r.Limits, "limits"); err != nil {
		return nil, err
	}
	return &out, nil
}

// ContainerInfo is a running worker pod.
type ContainerInfo struct {
	ContainerID string
	PodName     string
	Namespace   string
	WorkerURL   string
	PodIP       string
	Status      string
	SessionName string // raw session label (annotation), for display
}

// PodInfo is a pod summary (deployment replicas etc).
type PodInfo struct {
	Name     string
	IP       string
	Phase    string
	Ready    bool
	Image    string
	Age      string
	Restarts int32
}

// Manager drives pod + service lifecycle via client-go (no kubectl binary).
type Manager struct {
	client *kubernetes.Clientset
	config Config
}

// NewManager builds a Manager. Rest config resolution: explicit
// ZERGX_KUBECONFIG (verification instances), then in-cluster service
// account, then $KUBECONFIG / ~/.kube/config.
func NewManager(cfg Config) (*Manager, error) {
	var r *rest.Config
	var err error
	if explicit := os.Getenv("ZERGX_KUBECONFIG"); explicit != "" {
		r, err = clientcmd.BuildConfigFromFlags("", explicit)
	} else {
		r, err = rest.InClusterConfig()
		if err != nil {
			loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
			r, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{}).ClientConfig()
		}
	}
	if err != nil {
		return nil, err
	}
	cl, err := kubernetes.NewForConfig(r)
	if err != nil {
		return nil, err
	}
	return &Manager{client: cl, config: cfg}, nil
}

// Namespace returns the configured namespace.
func (m *Manager) Namespace() string { return m.config.Namespace }

// WorkerImage returns the configured worker image.
func (m *Manager) WorkerImage() string { return m.config.WorkerImage }

// podName derives the pod/service name for a container label. Hashing (not
// truncating) the label keeps distinct sessions collision-free: session names
// like "acme:repo-a:*" and "acme:repo-b:*" share long common prefixes.
func podName(containerID string) string {
	return "sandbox-" + labelKey(containerID)[:8]
}

// labelKey sanitizes an arbitrary label (session names contain ':' which is
// illegal in label values) into a deterministic k8s-safe key. Values that are
// already valid are used as-is (e.g. UUIDs from the HTTP face).
func labelKey(label string) string {
	if validLabelValue(label) {
		return label
	}
	sum := sha256.Sum256([]byte(label))
	return hex.EncodeToString(sum[:])[:16]
}

// validLabelValue follows the k8s label value grammar: alphanumerics, '-',
// '_' and '.', at most 63 chars.
func validLabelValue(v string) bool {
	if len(v) == 0 || len(v) > 63 {
		return false
	}
	for _, r := range v {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
			return false
		}
	}
	return true
}

// ListContainers returns all worker pods labelled app=sandbox.
func (m *Manager) ListContainers(ctx context.Context) ([]ContainerInfo, error) {
	list, err := m.client.CoreV1().Pods(m.config.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=sandbox",
	})
	if err != nil {
		return nil, err
	}
	out := make([]ContainerInfo, 0, len(list.Items))
	for _, p := range list.Items {
		out = append(out, containerInfoOf(p))
	}
	return out, nil
}

// EnsureContainer returns the running worker pod for a label, creating it if
// needed (get-or-create). The label may be a raw session name ("org:repo:bm",
// sanitized via labelKey) or an already-safe value (UUID). The raw name is
// preserved in the zergx/session annotation for display.
func (m *Manager) EnsureContainer(ctx context.Context, label, image string) (ContainerInfo, error) {
	key := labelKey(label)
	if info, err := m.FindContainer(ctx, key); err == nil {
		if info.WorkerURL != "" && info.Status == "running" {
			return info, nil
		}
		// Exists but not ready yet (or died): wait for it.
		return m.waitRunning(ctx, key, info.PodName)
	}

	if image == "" {
		image = m.config.WorkerImage
	}
	name := podName(key)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: m.config.Namespace,
			Labels: map[string]string{
				"app":               "sandbox",
				"zergx/container": key,
				labelOwned:          "true",
				labelSession:        key,
			},
			Annotations: map[string]string{
				annSession: label,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:            "worker",
					Image:           image,
					ImagePullPolicy: corev1.PullAlways,
					Ports:           []corev1.ContainerPort{{ContainerPort: workerPort}},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(orDefault(m.config.CPURequest, "250m")),
							corev1.ResourceMemory: resource.MustParse(orDefault(m.config.MemoryRequest, "256Mi")),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(orDefault(m.config.CPULimit, "1")),
							corev1.ResourceMemory: resource.MustParse(orDefault(m.config.MemoryLimit, "1Gi")),
						},
					},
					ReadinessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							TCPSocket: &corev1.TCPSocketAction{
								Port: intstr.FromInt(workerPort),
							},
						},
						InitialDelaySeconds: 2,
						PeriodSeconds:       5,
					},
				},
			},
		},
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: m.config.Namespace,
			Labels: map[string]string{
				"app":        "sandbox",
				labelOwned:   "true",
				labelSession: key,
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app":               "sandbox",
				"zergx/container": key,
			},
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Port:       80,
				TargetPort: intstr.FromInt(workerPort),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}

	// AlreadyExists means a concurrent path won the race — converge by waiting.
	if _, err := m.client.CoreV1().Pods(m.config.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return ContainerInfo{}, err
	}
	if _, err := m.client.CoreV1().Services(m.config.Namespace).Create(ctx, svc, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return ContainerInfo{}, err
	}

	return m.waitRunning(ctx, key, name)
}

// FindContainer returns the worker pod carrying the given container key, or
// an error when none exists.
func (m *Manager) FindContainer(ctx context.Context, key string) (ContainerInfo, error) {
	list, err := m.client.CoreV1().Pods(m.config.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=sandbox,zergx/container=" + key,
	})
	if err != nil {
		return ContainerInfo{}, err
	}
	if len(list.Items) == 0 {
		return ContainerInfo{}, fmt.Errorf("no sandbox pod for %q", key)
	}
	p := list.Items[0]
	return containerInfoOf(p), nil
}

// containerInfoOf maps a pod to its ContainerInfo.
func containerInfoOf(p corev1.Pod) ContainerInfo {
	ip := p.Status.PodIP
	url := ""
	if ip != "" {
		url = fmt.Sprintf("http://%s:%d", ip, workerPort)
	}
	session := p.Annotations[annSession]
	if session == "" {
		session = p.Labels["zergx/container"]
	}
	return ContainerInfo{
		ContainerID: p.Labels["zergx/container"],
		PodName:     p.Name,
		Namespace:   p.Namespace,
		WorkerURL:   url,
		PodIP:       ip,
		Status:      strings.ToLower(string(p.Status.Phase)),
		SessionName: session,
	}
}

func (m *Manager) waitRunning(ctx context.Context, containerID, name string) (ContainerInfo, error) {
	deadline := time.After(60 * time.Second)
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ContainerInfo{}, ctx.Err()
		case <-deadline:
			return ContainerInfo{}, fmt.Errorf("pod %s not ready within 60s", name)
		case <-tick.C:
			info, err := m.getContainer(ctx, containerID)
			if err == nil && info.Status == "running" {
				return info, nil
			}
		}
	}
}

func (m *Manager) getContainer(ctx context.Context, containerID string) (ContainerInfo, error) {
	list, err := m.ListContainers(ctx)
	if err != nil {
		return ContainerInfo{}, err
	}
	for _, c := range list {
		if c.ContainerID == containerID || c.PodName == podName(containerID) {
			return c, nil
		}
	}
	return ContainerInfo{}, fmt.Errorf("container %s not found", containerID)
}

// DestroyContainer deletes the pod + service for a container ID.
// DestroyContainer removes the worker pod + service. The ID may be the
// container UUID/label, the pod name, or the "sandbox-*" short form — other
// surfaces (resolveWorkerURL) accept any of these, so deletion must too.
func (m *Manager) DestroyContainer(ctx context.Context, id string) error {
	name := id
	if _, err := m.client.CoreV1().Pods(m.config.Namespace).Get(ctx, name, metav1.GetOptions{}); err != nil {
		// Not a literal pod name; resolve via label (key, raw session name,
		// or UUID) before falling back to the derived short form.
		name = ""
		for _, cand := range []string{id, labelKey(id)} {
			if pods, lerr := m.client.CoreV1().Pods(m.config.Namespace).List(ctx, metav1.ListOptions{
				LabelSelector: "app=sandbox,zergx/container=" + cand,
			}); lerr == nil && len(pods.Items) > 0 {
				name = pods.Items[0].Name
				break
			}
		}
		if name == "" {
			name = podName(id)
		}
	}
	perr := m.client.CoreV1().Pods(m.config.Namespace).Delete(ctx, name, metav1.DeleteOptions{})
	serr := m.client.CoreV1().Services(m.config.Namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if perr != nil && serr != nil {
		return fmt.Errorf("delete %s: pod: %v, service: %v", name, perr, serr)
	}
	return nil
}

// EnsureDeployment creates (or updates) a Deployment + Service for a registry
// image. `session` (raw "org:repo:bookmark") ties the deployment to a session;
// empty means an unowned deployment. Existing deployments are updated in place
// (image/replicas/env/port/resources) so a same-name redeploy triggers a
// normal rolling update; the Service keeps its immutable clusterIP.
// `resources`, when non-nil, overrides the manager's global defaults for this
// deployment only.
func (m *Manager) EnsureDeployment(ctx context.Context, name, image string, replicas, port int32, env map[string]string, session string, resources *corev1.ResourceRequirements) error {
	replicas = max(replicas, 1)

	envVars := make([]corev1.EnvVar, 0, len(env))
	for k, v := range env {
		envVars = append(envVars, corev1.EnvVar{Name: k, Value: v})
	}

	reqs := m.resourcesFor()
	if resources != nil {
		reqs = *resources
	}

	labels := map[string]string{
		"app":        name,
		labelOwned:   "true",
		labelSession: labelKey(session),
	}

	container := corev1.Container{
		Name:            name,
		Image:           image,
		ImagePullPolicy: corev1.PullAlways,
		Env:             envVars,
		Resources:       reqs,
		Ports:           []corev1.ContainerPort{{ContainerPort: port}},
	}

	deps := m.client.AppsV1().Deployments(m.config.Namespace)
	existing, err := deps.Get(ctx, name, metav1.GetOptions{})
	switch {
	case err == nil:
		// In-place update keeps resourceVersion + selector, preserving the
		// rollout semantics and the immutable selector.
		existing.Spec.Replicas = &replicas
		existing.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: labels},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{container}},
		}
		if existing.Labels == nil {
			existing.Labels = labels
		} else {
			for k, v := range labels {
				existing.Labels[k] = v
			}
		}
		if existing.Spec.Template.Annotations == nil {
			existing.Spec.Template.Annotations = map[string]string{}
		}
		if session != "" {
			existing.Spec.Template.Annotations[annSession] = session
		}
		if _, err := deps.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
			return err
		}
	case apierrors.IsNotFound(err):
		deploy := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: m.config.Namespace, Labels: labels},
			Spec: appsv1.DeploymentSpec{
				Replicas:             &replicas,
				RevisionHistoryLimit: ptrInt32(10),
				Selector:             &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels:      labels,
						Annotations: map[string]string{},
					},
					Spec: corev1.PodSpec{Containers: []corev1.Container{container}},
				},
			},
		}
		if session != "" {
			deploy.Spec.Template.Annotations[annSession] = session
		}
		if _, err := deps.Create(ctx, deploy, metav1.CreateOptions{}); err != nil {
			return err
		}
	default:
		return err
	}

	// Service: preserve clusterIP (immutable) on update.
	// An app container listens on its `port` (8080); the Service exposes it
	// on a uniform external port 80 so consumers always reach user deployments
	// at the well-known HTTP port regardless of the container port.
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: m.config.Namespace, Labels: labels},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": name},
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Port:       80,
				TargetPort: intstr.FromInt32(port),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}
	svcs := m.client.CoreV1().Services(m.config.Namespace)
	existingSvc, serr := svcs.Get(ctx, name, metav1.GetOptions{})
	switch {
	case serr == nil:
		svc.Spec.ClusterIP = existingSvc.Spec.ClusterIP
		svc.Spec.ClusterIPs = existingSvc.Spec.ClusterIPs
		svc.ObjectMeta.ResourceVersion = existingSvc.ObjectMeta.ResourceVersion
		if _, err := svcs.Update(ctx, svc, metav1.UpdateOptions{}); err != nil {
			return err
		}
	case apierrors.IsNotFound(serr):
		if _, err := svcs.Create(ctx, svc, metav1.CreateOptions{}); err != nil {
			return err
		}
	default:
		return serr
	}
	return nil
}

// DeploymentInfo is a summary of a deployed service.
type DeploymentInfo struct {
	Name      string
	Image     string
	Replicas  int32
	Ready     int32
	Namespace string
	Age       string
	Ports     []int32
	Session   string // raw session name (annotation), empty when unowned
	Resources ResourceRequest
}

// DeploymentStatus reports the rollout state of a deployment.
type DeploymentStatus struct {
	ObservedGeneration  int64
	UpdatedReplicas     int32
	ReadyReplicas       int32
	AvailableReplicas   int32
	UnavailableReplicas int32
	Conditions          []string
}

// ListDeployments returns every Deployment created via EnsureDeployment,
// identified by the owned label (never system/Helm deployments).
func (m *Manager) ListDeployments(ctx context.Context) ([]DeploymentInfo, error) {
	return m.listDeployments(ctx, "")
}

// FindDeploymentsBySession lists deployments owned by a session (raw name).
func (m *Manager) FindDeploymentsBySession(ctx context.Context, session string) ([]DeploymentInfo, error) {
	return m.listDeployments(ctx, labelKey(session))
}

func (m *Manager) listDeployments(ctx context.Context, sessionKey string) ([]DeploymentInfo, error) {
	sel := labelOwned + "=true"
	if sessionKey != "" {
		sel += "," + labelSession + "=" + sessionKey
	}
	list, err := m.client.AppsV1().Deployments(m.config.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: sel,
	})
	if err != nil {
		return nil, err
	}
	out := make([]DeploymentInfo, 0, len(list.Items))
	for _, d := range list.Items {
		spec := d.Spec.Template.Spec
		img := ""
		if len(spec.Containers) > 0 {
			img = spec.Containers[0].Image
		}
		replicas := int32(1)
		if d.Spec.Replicas != nil {
			replicas = *d.Spec.Replicas
		}
		var ports []int32
		for _, c := range spec.Containers {
			for _, p := range c.Ports {
				ports = append(ports, p.ContainerPort)
			}
		}
		session := d.Spec.Template.Annotations[annSession]
		out = append(out, DeploymentInfo{
			Name:      d.Name,
			Image:     img,
			Replicas:  replicas,
			Ready:     d.Status.ReadyReplicas,
			Namespace: d.Namespace,
			Age:       formatAge(d.CreationTimestamp.Time),
			Ports:     ports,
			Session:   session,
			Resources: resourcesFromContainer(spec),
		})
	}
	return out, nil
}

// resourcesFromContainer extracts the first container's resource requirements
// into the nested ResourceRequest shape (empty when unset).
func resourcesFromContainer(spec corev1.PodSpec) ResourceRequest {
	if len(spec.Containers) == 0 {
		return ResourceRequest{}
	}
	res := spec.Containers[0].Resources
	out := ResourceRequest{}
	if cpu, ok := res.Requests[corev1.ResourceCPU]; ok {
		if out.Requests == nil {
			out.Requests = &ResourcePair{}
		}
		out.Requests.CPU = cpu.String()
	}
	if mem, ok := res.Requests[corev1.ResourceMemory]; ok {
		if out.Requests == nil {
			out.Requests = &ResourcePair{}
		}
		out.Requests.Memory = mem.String()
	}
	if cpu, ok := res.Limits[corev1.ResourceCPU]; ok {
		if out.Limits == nil {
			out.Limits = &ResourcePair{}
		}
		out.Limits.CPU = cpu.String()
	}
	if mem, ok := res.Limits[corev1.ResourceMemory]; ok {
		if out.Limits == nil {
			out.Limits = &ResourcePair{}
		}
		out.Limits.Memory = mem.String()
	}
	return out
}

// DeploymentPods returns the pods of a deployment.
func (m *Manager) DeploymentPods(ctx context.Context, name string) ([]PodInfo, error) {
	list, err := m.client.CoreV1().Pods(m.config.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + name,
	})
	if err != nil {
		return nil, err
	}
	out := make([]PodInfo, 0, len(list.Items))
	for _, p := range list.Items {
		img := ""
		restarts := int32(0)
		if len(p.Spec.Containers) > 0 {
			img = p.Spec.Containers[0].Image
		}
		ready := false
		for _, c := range p.Status.ContainerStatuses {
			restarts += c.RestartCount
			if c.Ready {
				ready = true
			}
		}
		out = append(out, PodInfo{
			Name:     p.Name,
			IP:       p.Status.PodIP,
			Phase:    string(p.Status.Phase),
			Ready:    ready,
			Image:    img,
			Age:      formatAge(p.CreationTimestamp.Time),
			Restarts: restarts,
		})
	}
	return out, nil
}

// DeploymentStatus reports the rollout state of a deployment.
func (m *Manager) DeploymentStatus(ctx context.Context, name string) (DeploymentStatus, error) {
	d, err := m.client.AppsV1().Deployments(m.config.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return DeploymentStatus{}, err
	}
	conds := make([]string, 0, len(d.Status.Conditions))
	for _, c := range d.Status.Conditions {
		conds = append(conds, fmt.Sprintf("%s=%s (%s)", c.Type, c.Status, c.Reason))
	}
	return DeploymentStatus{
		ObservedGeneration:  d.Status.ObservedGeneration,
		UpdatedReplicas:     d.Status.UpdatedReplicas,
		ReadyReplicas:       d.Status.ReadyReplicas,
		AvailableReplicas:   d.Status.AvailableReplicas,
		UnavailableReplicas: d.Status.UnavailableReplicas,
		Conditions:          conds,
	}, nil
}

// DeleteDeployment removes a deployment and its service.
func (m *Manager) DeleteDeployment(ctx context.Context, name string) error {
	derr := m.client.AppsV1().Deployments(m.config.Namespace).Delete(ctx, name, metav1.DeleteOptions{})
	serr := m.client.CoreV1().Services(m.config.Namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if derr != nil && serr != nil {
		return fmt.Errorf("delete %s: deployment: %v, service: %v", name, derr, serr)
	}
	return nil
}

// RestartDeployment triggers a rolling restart by bumping the
// `kubectl.kubernetes.io/restartedAt` annotation on the pod template — the
// same mechanism `kubectl rollout restart` uses.
func (m *Manager) RestartDeployment(ctx context.Context, name string) error {
	deps := m.client.AppsV1().Deployments(m.config.Namespace)
	d, err := deps.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if d.Spec.Template.Annotations == nil {
		d.Spec.Template.Annotations = map[string]string{}
	}
	d.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().UTC().Format(time.RFC3339)
	_, err = deps.Update(ctx, d, metav1.UpdateOptions{})
	return err
}

// ScaleDeployment sets the replica count of a deployment.
func (m *Manager) ScaleDeployment(ctx context.Context, name string, replicas int32) error {
	if replicas < 0 {
		return fmt.Errorf("replicas must be >= 0")
	}
	deps := m.client.AppsV1().Deployments(m.config.Namespace)
	d, err := deps.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	d.Spec.Replicas = &replicas
	_, err = deps.Update(ctx, d, metav1.UpdateOptions{})
	return err
}

// RollbackDeployment rolls a deployment back to a previous revision by
// replaying the stored ReplicaSet template (revision 0 = previous revision).
// Requires RevisionHistoryLimit > 0 (EnsureDeployment sets 10).
func (m *Manager) RollbackDeployment(ctx context.Context, name string, revision int64) error {
	rsList, err := m.client.AppsV1().ReplicaSets(m.config.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + name,
	})
	if err != nil {
		return err
	}

	rsRevision := func(rs *appsv1.ReplicaSet) int64 {
		if rs.Annotations == nil {
			return 0
		}
		rev, _ := strconv.ParseInt(rs.Annotations["deployment.kubernetes.io/revision"], 10, 64)
		return rev
	}

	var target *appsv1.ReplicaSet
	switch {
	case revision > 0:
		for i := range rsList.Items {
			if rsRevision(&rsList.Items[i]) == revision {
				target = &rsList.Items[i]
				break
			}
		}
		if target == nil {
			return fmt.Errorf("revision %d not found", revision)
		}
	case revision == 0:
		d, err := m.client.AppsV1().Deployments(m.config.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		curRevision := int64(0)
		if d.Annotations != nil {
			curRevision, _ = strconv.ParseInt(d.Annotations["deployment.kubernetes.io/revision"], 10, 64)
		}
		best := int64(-1)
		for i := range rsList.Items {
			rev := rsRevision(&rsList.Items[i])
			if rev < curRevision && rev > best {
				best = rev
				target = &rsList.Items[i]
			}
		}
		if target == nil {
			return fmt.Errorf("no previous revision to roll back to")
		}
	default:
		return fmt.Errorf("revision must be >= 0")
	}

	// Replay the target ReplicaSet's template into the deployment.
	d, err := m.client.AppsV1().Deployments(m.config.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	d.Spec.Template = *target.Spec.Template.DeepCopy()
	// Preserve the owned labels (selector must stay stable).
	if d.Spec.Template.Labels == nil {
		d.Spec.Template.Labels = map[string]string{}
	}
	d.Spec.Template.Labels["app"] = name
	_, err = m.client.AppsV1().Deployments(m.config.Namespace).Update(ctx, d, metav1.UpdateOptions{})
	return err
}

// DeploymentEvent is a k8s Event associated with a deployment's rollout.
type DeploymentEvent struct {
	Reason  string
	Message string
	Type    string
	Age     string
}

// DeploymentEvents lists events for a deployment's pods and ReplicaSets (the
// "why is my rollout stuck" view).
func (m *Manager) DeploymentEvents(ctx context.Context, name string) ([]DeploymentEvent, error) {
	events, err := m.client.CoreV1().Events(m.config.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := []DeploymentEvent{}
	for _, e := range events.Items {
		// Involved object is either the deployment, one of its ReplicaSets
		// (name prefixed by the deployment name), or one of its pods (RS name
		// prefix). Filter heuristically on the name prefix.
		obj := e.InvolvedObject.Name
		if obj == name || strings.HasPrefix(obj, name+"-") {
			out = append(out, DeploymentEvent{
				Reason:  e.Reason,
				Message: e.Message,
				Type:    e.Type,
				Age:     formatAge(e.LastTimestamp.Time),
			})
		}
	}
	return out, nil
}

// DeploymentRevisions lists the ReplicaSet revisions of a deployment,
// newest-first.
func (m *Manager) DeploymentRevisions(ctx context.Context, name string) ([]DeploymentRevision, error) {
	rsList, err := m.client.AppsV1().ReplicaSets(m.config.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + name,
	})
	if err != nil {
		return nil, err
	}
	out := []DeploymentRevision{}
	for i := range rsList.Items {
		rs := &rsList.Items[i]
		rev := int64(0)
		if rs.Annotations != nil {
			rev, _ = strconv.ParseInt(rs.Annotations["deployment.kubernetes.io/revision"], 10, 64)
		}
		img := ""
		if len(rs.Spec.Template.Spec.Containers) > 0 {
			img = rs.Spec.Template.Spec.Containers[0].Image
		}
		out = append(out, DeploymentRevision{
			Revision: rev,
			Image:    img,
			Replicas: rs.Status.Replicas,
			Ready:    rs.Status.ReadyReplicas,
			Age:      formatAge(rs.CreationTimestamp.Time),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Revision > out[j].Revision })
	return out, nil
}

// DeploymentRevision is one ReplicaSet revision of a deployment.
type DeploymentRevision struct {
	Revision int64
	Image    string
	Replicas int32
	Ready    int32
	Age      string
}

func formatAge(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	default:
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	}
}

func max(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

func ptrInt32(v int32) *int32 { return &v }

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

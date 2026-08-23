// Package k8s wraps client-go for dynamically starting/stopping worker pods.
package k8s

import (
	"context"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"os"
)

const (
	workerPort = 8080
)

// Config mirrors K8sConfig in the original executor.
type Config struct {
	Namespace   string
	WorkerImage string
	// Resource requests/limits for worker containers. Namespaces with a
	// ResourceQuota (like temp) reject pods without them.
	CPURequest    string
	CPULimit      string
	MemoryRequest string
	MemoryLimit   string
}

// ContainerInfo is a running worker pod.
type ContainerInfo struct {
	ContainerID string
	PodName     string
	Namespace   string
	WorkerURL   string
	PodIP       string
	Status      string
}

// Manager drives pod + service lifecycle via client-go (no kubectl binary).
type Manager struct {
	client *kubernetes.Clientset
	config Config
}

// NewManager builds a Manager. Rest config resolution: explicit
// RUCODER_KUBECONFIG (verification instances), then in-cluster service
// account, then $KUBECONFIG / ~/.kube/config.
func NewManager(cfg Config) (*Manager, error) {
	var r *rest.Config
	var err error
	if explicit := os.Getenv("RUCODER_KUBECONFIG"); explicit != "" {
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

func podName(containerID string) string {
	s := strings.ToLower(containerID)
	s = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			return r
		}
		return '-'
	}, s)
	s = strings.Trim(s, "-")
	if len(s) > 8 {
		s = s[:8]
	}
	return "sandbox-" + s
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
		ip := p.Status.PodIP
		phase := strings.ToLower(string(p.Status.Phase))
		url := ""
		if ip != "" {
			url = fmt.Sprintf("http://%s:%d", ip, workerPort)
		}
		out = append(out, ContainerInfo{
			ContainerID: p.Labels["rucoder/container"],
			PodName:     p.Name,
			Namespace:   m.config.Namespace,
			WorkerURL:   url,
			PodIP:       ip,
			Status:      phase,
		})
	}
	return out, nil
}

// EnsureContainer creates a worker pod + service and waits until running.
func (m *Manager) EnsureContainer(ctx context.Context, containerID, image string) (ContainerInfo, error) {
	if image == "" {
		image = m.config.WorkerImage
	}
	name := podName(containerID)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: m.config.Namespace,
			Labels: map[string]string{
				"app":               "sandbox",
				"rucoder/container": containerID,
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
			Labels:    map[string]string{"app": "sandbox"},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app":               "sandbox",
				"rucoder/container": containerID,
			},
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Port:       80,
				TargetPort: intstr.FromInt(workerPort),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}

	if _, err := m.client.CoreV1().Pods(m.config.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return ContainerInfo{}, err
	}
	if _, err := m.client.CoreV1().Services(m.config.Namespace).Create(ctx, svc, metav1.CreateOptions{}); err != nil {
		return ContainerInfo{}, err
	}

	return m.waitRunning(ctx, containerID, name)
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
func (m *Manager) DestroyContainer(ctx context.Context, containerID string) error {
	name := containerID
	if _, err := m.client.CoreV1().Pods(m.config.Namespace).Get(ctx, name, metav1.GetOptions{}); err != nil {
		// Not a literal pod name; resolve via the label (UUID/session) or the
		// derived short form.
		if pods, lerr := m.client.CoreV1().Pods(m.config.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: "app=sandbox,rucoder/container=" + containerID,
		}); lerr == nil && len(pods.Items) > 0 {
			name = pods.Items[0].Name
		} else {
			name = podName(containerID)
		}
	}
	perr := m.client.CoreV1().Pods(m.config.Namespace).Delete(ctx, name, metav1.DeleteOptions{})
	serr := m.client.CoreV1().Services(m.config.Namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if perr != nil && serr != nil {
		return fmt.Errorf("delete %s: pod: %v, service: %v", name, perr, serr)
	}
	return nil
}

// EnsureDeployment creates (or replaces) a Deployment + Service for a registry
// image, mirroring rucoder-k8s::ensure_deployment.
func (m *Manager) EnsureDeployment(ctx context.Context, name, image string, replicas, port int32, env map[string]string) error {
	replicas = max(replicas, 1)

	envVars := make([]corev1.EnvVar, 0, len(env))
	for k, v := range env {
		envVars = append(envVars, corev1.EnvVar{Name: k, Value: v})
	}

	labels := map[string]string{"app": name}

	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: m.config.Namespace, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:            name,
						Image:           image,
						ImagePullPolicy: corev1.PullAlways,
						Env:             envVars,
						Ports:           []corev1.ContainerPort{{ContainerPort: port}},
					}},
				},
			},
		},
	}

	deps := m.client.AppsV1().Deployments(m.config.Namespace)
	_, err := deps.Create(ctx, deploy, metav1.CreateOptions{})
	if err != nil {
		_, replErr := deps.Update(ctx, deploy, metav1.UpdateOptions{})
		if replErr != nil {
			return replErr
		}
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: m.config.Namespace, Labels: labels},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Name:       "http",
				Port:       port,
				TargetPort: intstr.FromInt32(port),
				Protocol:   corev1.ProtocolTCP,
			}},
		},
	}
	svcs := m.client.CoreV1().Services(m.config.Namespace)
	if _, err := svcs.Create(ctx, svc, metav1.CreateOptions{}); err != nil {
		_, replErr := svcs.Update(ctx, svc, metav1.UpdateOptions{})
		if replErr != nil {
			return replErr
		}
	}
	return nil
}

func max(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

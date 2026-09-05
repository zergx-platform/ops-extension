package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	abcprotocol "github.com/abcp-sdk/abc-protocol-go"
	"github.com/abcp-sdk/abc-protocol-go/extension"

	"github.com/zergx-platform/ops-extension/internal/worker"
)

// handlers returns the NATS tool handlers. Descriptions/schemas live in
// manifest.yaml (the single declarative protocol source); each handler is
// bound by tool name. Sandbox tools are session-scoped: ops-extension
// resolves the workspace via jjlab, lazily creates/reuses the session's
// worker pod, and syncs the repo tree into it (overlay-only, sandbox-only
// files are never deleted) before running.
func (s *server) handlers() map[string]extension.ToolSpec {
	return map[string]extension.ToolSpec{
		"sandbox-create": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				image := strArg(args, "image")
				if image == "" {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "sandbox-create: missing 'image' (base image jjlab can pull)", "sandbox-create：缺少 'image'（jjlab 可拉取的基础镜像）")
				}
				_, sid, err := s.resolveWorkspace(ctx, args, sessionName)
				if err != nil {
					return extension.ToolResultData{}, err
				}
				info, err := s.createWorker(ctx, sid, image)
				if err != nil {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "sandbox-create failed: %v", "sandbox-create 失败：%v", err)
				}
				s.publishSandboxVars(ctx, sid, info)
				return extension.ToolResultData{Content: lc(ctx, s.ext, sessionName,
					fmt.Sprintf("Created sandbox from %s (container %s, status %s).", image, info.ContainerID, info.Status),
					fmt.Sprintf("已从 %s 创建沙箱（容器 %s，状态 %s）。", image, info.ContainerID, info.Status))}, nil
			},
		},
		"sandbox-run": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				command := strArg(args, "command")
				if command == "" {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "sandbox-run: missing 'command'", "sandbox-run：缺少 'command'")
				}
				sc, err := s.ensureSandbox(ctx, args, sessionName, true)
				if err != nil {
					return extension.ToolResultData{}, err
				}
				workerURL, err := s.resolveWorkerURL(ctx, sc.cid)
				if err != nil {
					return extension.ToolResultData{}, err
				}

				run := func(rev string) (worker.ExecuteResult, error) {
					return worker.Execute(ctx, worker.ToWsURL(workerURL), command, rev)
				}

				res, err := run(sc.ws.rev)
				if err != nil {
					// Worker may have restarted (synced_rev lost): re-sync once
					// and retry; if it still refuses, execute without the rev
					// gate (content is verified synced on our side).
					if strings.Contains(err.Error(), "need_sync") {
						s.markUnsynced(sc.cid)
						if err := s.ensureSynced(ctx, sc.cid, sc.session, sc.ws); err != nil {
							return extension.ToolResultData{}, err
						}
						if res, err = run(sc.ws.rev); err != nil {
							if res, err = run(""); err != nil {
								return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "sandbox-run failed: %v", "sandbox-run 失败：%v", err)
							}
						}
					} else {
						return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "sandbox-run failed: %v", "sandbox-run 失败：%v", err)
					}
				}

				// Sync-wait the job up to timeout_ms (default 10s). The worker
				// always registers a backgrounded job; the per-job SSE stream
				// replays history then streams live output until job.completed.
				// The bus is model-facing and carries no streamed deltas, so
				// the terminal tool result folds the captured output in; the
				// UI reads live output via the gateway's per-worker SSE proxy.
				timeoutMs := int(abcprotocol.ArgInt(args, "timeout-ms", 10000))
				if timeoutMs <= 0 {
					timeoutMs = 10000
				}
				streamCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutMs)*time.Millisecond)
				defer cancel()

				type streamResult struct {
					done worker.JobDone
					err  error
				}
				resultCh := make(chan streamResult, 1)
				go func() {
					done, err := worker.StreamJobOutput(streamCtx, workerURL, res.JobID, nil)
					resultCh <- streamResult{done: done, err: err}
				}()

				select {
				case sr := <-resultCh:
					if sr.err != nil && !errors.Is(sr.err, context.DeadlineExceeded) {
						return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "sandbox-run stream failed: %v", "sandbox-run 流失败：%v", sr.err)
					}
					content := fmt.Sprintf("Command completed (job %s, exit %d)", res.JobID, sr.done.ExitCode)
					if sr.done.Stdout != "" {
						content += "\n" + sr.done.Stdout
					}
					if sr.done.Stderr != "" {
						content += "\n[stderr]\n" + sr.done.Stderr
					}
					return extension.ToolResultData{Content: content, Data: map[string]interface{}{
						"job-id":       res.JobID,
						"exit_code":    sr.done.ExitCode,
						"backgrounded": false,
					}}, nil

				case <-streamCtx.Done():
					// Timed out: hand the job to a background watcher and return
					// immediately. The watcher keeps the SSE stream open until
					// completion, then notifies the agent via the session
					// mailbox (payload.content is folded into the chat).
					if s.ext != nil {
						go func(jobID, sid string) {
							bgCtx, bgCancel := context.WithTimeout(context.Background(), 30*time.Minute)
							defer bgCancel()
							done, streamErr := worker.StreamJobOutput(bgCtx, workerURL, jobID, nil)
							if streamErr != nil {
								// Never report a fabricated "finished (exit 0)"
								// when the stream broke — tell the agent the
								// outcome is unknown and how to inspect it.
								_ = s.ext.PublishMailboxEvent(context.Background(), sid, "event",
									map[string]interface{}{"content": fmt.Sprintf(
										"Background command stream failed (job %s): %v. The job itself may still be running; inspect it with sandbox-job-output or stop it with sandbox-job-kill.",
										jobID, streamErr)})
								return
							}
							msg := fmt.Sprintf("Background command finished (job %s, exit %d)", jobID, done.ExitCode)
							if done.Stdout != "" {
								msg += "\n" + done.Stdout
							}
							if done.Stderr != "" {
								msg += "\n[stderr]\n" + done.Stderr
							}
							_ = s.ext.PublishMailboxEvent(context.Background(), sid, "event",
								map[string]interface{}{"content": msg})
						}(res.JobID, sessionName)
					}
					content := fmt.Sprintf(
						"Command is still running in the background (job %s); it did not finish within %dms. It keeps running in the background and you will be notified on completion. Meanwhile you can inspect current output with sandbox-job-output, or stop it with sandbox-job-kill.",
						res.JobID, timeoutMs)
					return extension.ToolResultData{Content: content, Data: map[string]interface{}{
						"job-id":       res.JobID,
						"backgrounded": true,
					}}, nil
				}
			},
		},
		"sandbox-read": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				path := strArg(args, "path")
				sc, err := s.ensureSandbox(ctx, args, sessionName, true)
				if err != nil {
					return extension.ToolResultData{}, err
				}
				data, err := s.sandboxFileRead(ctx, sc.cid, path)
				if err != nil {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "sandbox-read failed: %v", "sandbox-read 失败：%v", err)
				}
				return extension.ToolResultData{Content: string(data)}, nil
			},
		},
		"sandbox-download": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				code := strArg(args, "code")
				path := strArg(args, "path")
				if code == "" || path == "" {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "sandbox-download: 'code' and 'path' are required", "sandbox-download：'code' 与 'path' 均为必填")
				}
				sc, err := s.ensureSandbox(ctx, args, sessionName, false)
				if err != nil {
					return extension.ToolResultData{}, err
				}
				data, err := s.fetchAgentFile(ctx, code)
				if err != nil {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "sandbox-download: download %s: %v", "sandbox-download：下载 %s：%v", code, err)
				}
				if err := s.sandboxFileWrite(ctx, sc.cid, path, data); err != nil {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "sandbox-download failed: %v", "sandbox-download 失败：%v", err)
				}
				return extension.ToolResultData{Content: lc(ctx, s.ext, sessionName, fmt.Sprintf("Downloaded file %s → sandbox path '%s' (%d bytes).", code, path, len(data)), fmt.Sprintf("已将文件 %s 下载到沙箱路径 '%s'（%d 字节）。", code, path, len(data)))}, nil
			},
		},
		"sandbox-write": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				path := strArg(args, "path")
				sc, err := s.ensureSandbox(ctx, args, sessionName, false)
				if err != nil {
					return extension.ToolResultData{}, err
				}
				if err := s.sandboxFileWrite(ctx, sc.cid, path, []byte(strArg(args, "content"))); err != nil {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "sandbox-write failed: %v", "sandbox-write 失败：%v", err)
				}
				return extension.ToolResultData{Content: lc(ctx, s.ext, sessionName, fmt.Sprintf("Wrote sandbox file '%s'.", path), fmt.Sprintf("已写入沙箱文件 '%s'。", path))}, nil
			},
		},
		"sandbox-edit": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				path := strArg(args, "path")
				startLine := intArg64(args, "start-line", 0)
				endLine := intArg64(args, "end-line", 0)
				content := strArg(args, "content")
				sc, err := s.ensureSandbox(ctx, args, sessionName, true)
				if err != nil {
					return extension.ToolResultData{}, err
				}
				v, err := s.sandboxEdit(ctx, sessionName, sc.cid, path, startLine, endLine, content)
				return extension.ToolResultData{Content: v}, err
			},
		},
		"sandbox-job-list": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				sc, err := s.ensureSandbox(ctx, args, sessionName, false)
				if err != nil {
					return extension.ToolResultData{}, err
				}
				res, err := s.workerCommand(ctx, sc.cid, "jobs", map[string]interface{}{})
				if err != nil {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "sandbox-job-list failed: %v", "sandbox-job-list 失败：%v", err)
				}
				return extension.ToolResultData{Content: toJSON(res)}, nil
			},
		},
		"sandbox-job-output": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				sc, err := s.ensureSandbox(ctx, args, sessionName, false)
				if err != nil {
					return extension.ToolResultData{}, err
				}
				res, err := s.workerCommand(ctx, sc.cid, "job_output", jobArgs(args))
				if err != nil {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "sandbox-job-output failed: %v", "sandbox-job-output 失败：%v", err)
				}
				return extension.ToolResultData{Content: toJSON(res)}, nil
			},
		},
		"sandbox-job-wait": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				sc, err := s.ensureSandbox(ctx, args, sessionName, false)
				if err != nil {
					return extension.ToolResultData{}, err
				}
				res, err := s.workerCommand(ctx, sc.cid, "job_wait", jobArgs(args))
				if err != nil {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "sandbox-job-wait failed: %v", "sandbox-job-wait 失败：%v", err)
				}
				return extension.ToolResultData{Content: toJSON(res)}, nil
			},
		},
		"sandbox-job-stdin": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				sc, err := s.ensureSandbox(ctx, args, sessionName, false)
				if err != nil {
					return extension.ToolResultData{}, err
				}
				res, err := s.workerCommand(ctx, sc.cid, "job_stdin", map[string]interface{}{
					"job_id": strArg(args, "job-id"),
					"data":   strArg(args, "data"),
				})
				if err != nil {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "sandbox-job-stdin failed: %v", "sandbox-job-stdin 失败：%v", err)
				}
				return extension.ToolResultData{Content: toJSON(res)}, nil
			},
		},
		"sandbox-job-kill": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				sc, err := s.ensureSandbox(ctx, args, sessionName, false)
				if err != nil {
					return extension.ToolResultData{}, err
				}
				res, err := s.workerCommand(ctx, sc.cid, "kill", map[string]interface{}{
					"job_id": strArg(args, "job-id"),
				})
				if err != nil {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "sandbox-job-kill failed: %v", "sandbox-job-kill 失败：%v", err)
				}
				return extension.ToolResultData{Content: toJSON(res)}, nil
			},
		},
		"sandbox-port": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				sc, err := s.ensureSandbox(ctx, args, sessionName, false)
				if err != nil {
					return extension.ToolResultData{}, err
				}
				v, err := s.portFile(ctx, sessionName, sc, args)
				if err == nil {
					// The bookmark moved: forget the cached head so the next
					// call re-syncs and observes the ported file.
					s.invalidateWorkspace(sc.session)
				}
				return extension.ToolResultData{Content: v}, err
			},
		},
		"container-build": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				ws, _, err := s.resolveWorkspace(ctx, args, sessionName)
				if err != nil {
					return extension.ToolResultData{}, err
				}
				image := strArg(args, "tag")
				if image == "" {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "container-build: missing 'tag' (image name)", "container-build：缺少 'tag'（镜像名）")
				}
				// Tag defaults to the session bookmark (matching container-build's
				// historical {tag}:{bookmark}); an explicit image_tag overrides it.
				ref := image + ":" + ws.bookmarkOrDefault()
				if imageTag := strArg(args, "image-tag"); imageTag != "" {
					ref = image + ":" + imageTag
				}
				fullImage := s.artifactImageHost + "/" + ref
				payload := map[string]interface{}{
					"org":      ws.org,
					"repo":     ws.repo,
					"bookmark": ws.bookmark,
					"image":    fullImage,
					"export":   "push",
					"no-cache": boolArg(args, "no-cache"),
				}
				if df := strArg(args, "dockerfile-path"); df != "" {
					payload["dockerfile"] = df
				}
				id, err := s.opsSubmitBuild(ctx, payload)
				if err != nil {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "container-build failed: %v", "container-build 失败：%v", err)
				}
				out, err := s.awaitOpsTaskProgress(ctx, "build", fullImage, id, callID)
				return extension.ToolResultData{Content: out}, err
			},
		},
		"service-deploy": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				image := strArg(args, "image")
				name := strArg(args, "name")
				defaultTag := ""
				org, repo, bm := "", "", ""
				if sessionName != "" {
					if o, r, b, ok := parseSessionName(sessionName); ok {
						org, repo, bm = o, r, b
						defaultTag = b
					}
				}
				// Default the service name to a k8s-safe, globally-unique slug so
				// deployments never collide across orgs/repos/sessions. A caller
				// may still pass an explicit `name` (ownership check still applies).
				if name == "" && org != "" && repo != "" && bm != "" {
					name = k8sServiceName(org, repo, bm)
				}
				if name == "" {
					name = "app"
				}
				// Ownership guard: only the session that created a service may
				// update/scale it. Read the existing service's session annotation
				// (jjlab status now returns `annotations`); absent annotation
				// (legacy/service, no zergx/session) is adopted + tagged on first
				// touch; a mismatched session is rejected with a conflict.
				existing, err := s.fetchService(ctx, name, s.runtimeNamespace)
				if err == nil && existing != nil {
					owner, _ := existing["session"].(string)
					if owner != "" && owner != sessionName {
						return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "service '%s' belongs to a different session (%s); use another name", "服务 '%s' 属于其它会话（%s）；请换一个名字", name, owner)
					}
				}
				image = s.qualifyImage(image, defaultTag)
				rr := resourceRequestFromArgs(args)
				body := map[string]interface{}{
					"name":  name,
					"image": image,
					"kind":  "deployment",
					"ports": []map[string]interface{}{{"container": 8080, "service": 80}},
				}
				ann := map[string]string{}
				if sessionName != "" {
					ann["zergx/session"] = sessionName
				}
				if org != "" {
					ann["zergx/org"] = org
				}
				if repo != "" {
					ann["zergx/repo"] = repo
				}
				if len(ann) > 0 {
					body["annotations"] = ann
				}
				if env := envMapFromArgs(args); len(env) > 0 {
					body["env"] = env
				}
				if rr.Requests != nil || rr.Limits != nil {
					res := map[string]interface{}{}
					if rr.Requests != nil {
						res["cpu"] = rr.Requests.CPU
						res["memory"] = rr.Requests.Memory
					}
					body["resources"] = res
				}
				body["namespace"] = s.runtimeNamespace
				resp, err := s.httpPostJSON(ctx, s.jj+"/api/v1/ops/services", body)
				if err != nil {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "service-deploy failed: %v", "service-deploy 失败：%v", err)
				}
				// Surface the service's in-cluster DNS address + readiness so the
				// sandbox (same runtime namespace) can reach it by name, and the
				// agent knows the endpoint to reference.
				svcHost := fmt.Sprintf("%s.%s.svc.cluster.local", name, s.runtimeNamespace)
				ready := "unknown"
				var pm map[string]interface{}
				if jerr := json.Unmarshal([]byte(resp), &pm); jerr == nil {
					if r, ok := pm["ready"].(bool); ok {
						ready = fmt.Sprintf("%v", r)
					}
				}
				return extension.ToolResultData{Content: lc(ctx, s.ext, sessionName,
					fmt.Sprintf("Deployed '%s' from %s. In-cluster address: http://%s:80 (ready=%s). The sandbox can reach it via this hostname.", name, image, svcHost, ready),
					fmt.Sprintf("已从 %s 部署 '%s'。集群内地址：http://%s:80（ready=%s）。沙箱可直接用该主机名访问。", image, name, svcHost, ready)),
					Data: map[string]interface{}{"name": name, "image": image, "svc": svcHost, "url": "http://" + svcHost + ":80", "ready": ready}}, nil
			},
		},
		"container-search": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				// Search the OCI image registry. Default: all cached/published
				// images (incl. library/* base images like go:alpine). Optional
				// repo=<org/repo> filters to images built from that source repo;
				// source=push|pull filters origin; all=true returns everything.
				repo := strArg(args, "repo")
				src := strArg(args, "source")
				q := url.Values{}
				if repo != "" {
					q.Set("repo", repo)
				}
				if src != "" {
					q.Set("source", src)
				}
				all := boolArg(args, "all")
				if all {
					q.Set("all", "1")
				}
				u := s.jj + "/api/v1/ops/images"
				if s := q.Encode(); s != "" {
					u += "?" + s
				}
				v, err := s.httpGetJSON(ctx, u)
				if err != nil {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "container-search failed: %v", "container-search 失败：%v", err)
				}
				return extension.ToolResultData{Content: v}, nil
			},
		},
		"service-list": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				all := boolArg(args, "all")
				org := strArg(args, "org")
				repo := strArg(args, "repo")
				kind := strArg(args, "kind")
				u := s.jj + "/api/v1/ops/services"
				if ns := strArg(args, "namespace"); ns != "" {
					u += "?namespace=" + url.QueryEscape(ns)
				}
				v, err := s.httpGetJSON(ctx, u)
				if err != nil {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "service-list failed: %v", "service-list 失败：%v", err)
				}
				if !all && sessionName != "" {
					if _, _, _, ok := tryParseSession(sessionName); ok && v != "" {
						if filtered := filterServicesBySession(v, sessionName); filtered != "" {
							v = filtered
						}
					}
				}
				// Additional tool-layer filters (org/repo/kind) over the returned
				// annotations/kind. jjlab list returns these; we slice here so the
				// caller can narrow to e.g. only formal services (kind=deployment)
				// or a specific org/repo without backend support.
				if org != "" || repo != "" || kind != "" {
					v = filterServicesByExtra(v, org, repo, kind)
				}
				return extension.ToolResultData{Content: v}, nil
			},
		},
		"helm-install": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				release := strArg(args, "release-name")
				if release == "" {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "helm-install: missing 'release_name'", "helm-install：缺少 'release_name'")
				}
				// chart_path is repo-relative; jjlab materializes an absolute
				// chart directory from the session's workspace when we pass the
				// org/repo/bookmark (its helm v4 can't resolve a bare relative
				// path or an archive URL).
				chart := strArg(args, "chart-path")
				if chart == "" {
					chart = strArg(args, "chart")
				}
				payload := map[string]interface{}{
					"release_name": release,
					"chart":        chart,
				}
				if ws, _, werr := s.resolveWorkspace(ctx, args, sessionName); werr == nil {
					payload["org"] = ws.org
					payload["repo"] = ws.repo
					payload["bookmark"] = ws.bookmark
				} else {
					payload["namespace"] = s.runtimeNamespace
				}
				if v := args["values"]; v != nil {
					payload["values"] = v
				}
				id, err := s.opsSubmitHelm(ctx, payload)
				if err != nil {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "helm-install failed: %v", "helm-install 失败：%v", err)
				}
				out, err := s.awaitOpsTask(ctx, "helm", release, id, callID)
				return extension.ToolResultData{Content: out}, err
			},
		},
		"helm-list": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				v, err := s.httpGetJSON(ctx, s.jj+"/api/v1/ops/helm/releases?namespace="+url.QueryEscape(s.runtimeNamespace))
				return extension.ToolResultData{Content: v}, err
			},
		},
		"helm-status": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				name := strArg(args, "release-name")
				if name == "" {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "helm-status: missing 'release_name'", "helm-status：缺少 'release_name'")
				}
				v, err := s.httpGetJSON(ctx, s.jj+"/api/v1/ops/helm/releases/"+urlPathEscape(name)+"?namespace="+url.QueryEscape(s.runtimeNamespace))
				return extension.ToolResultData{Content: v}, err
			},
		},
		"helm-uninstall": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				name := strArg(args, "release-name")
				if name == "" {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "helm-uninstall: missing 'release_name'", "helm-uninstall：缺少 'release_name'")
				}
				if err := s.httpDelete(ctx, s.jj+"/api/v1/ops/helm/releases/"+urlPathEscape(name)+"?namespace="+url.QueryEscape(s.runtimeNamespace)); err != nil {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "helm-uninstall failed: %v", "helm-uninstall 失败：%v", err)
				}
				return extension.ToolResultData{Content: lc(ctx, s.ext, sessionName, fmt.Sprintf("Uninstalled helm release %q", name), fmt.Sprintf("已卸载 Helm release %q", name))}, nil
			},
		},
		"package-publish": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				protocol := strArg(args, "protocol")
				org, repo, bookmark := strArg(args, "org"), strArg(args, "repo"), strArg(args, "bookmark")
				if org == "" || repo == "" {
					ws, _, err := s.resolveWorkspace(ctx, args, sessionName)
					if err != nil {
						return extension.ToolResultData{}, err
					}
					if org == "" {
						org, repo = ws.org, ws.repo
					}
					if bookmark == "" {
						bookmark = ws.bookmark
					}
				}
				res, err := s.publishPackage(ctx, protocol, org, repo, bookmark,
					strArg(args, "name"), strArg(args, "version"),
					strArg(args, "file"), strArg(args, "dockerfile-path"))
				return extension.ToolResultData{Content: res}, err
			},
		},
		"package-search": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				u := s.jj + "/api/v1/ops/packages"
				if p := strArg(args, "protocol"); p != "" {
					u += "?protocol=" + url.QueryEscape(p)
				}
				v, err := s.httpGetJSON(ctx, u)
				if err != nil {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "package-search failed: %v", "package-search 失败：%v", err)
				}
				return extension.ToolResultData{Content: v}, nil
			},
		},
		"pull-git-repo": {
			Execute: func(ctx context.Context, args map[string]interface{}, callID string, sessionName string) (extension.ToolResultData, error) {
				gitURL := strArg(args, "git-url")
				if gitURL == "" {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "pull-git-repo: missing 'git_url'", "pull-git-repo：缺少 'git_url'")
				}
				repo := inferRepoFromGitURL(gitURL)
				if repo == "" {
					return extension.ToolResultData{}, ef(ctx, s.ext, sessionName, "cannot infer repo name from %s", "无法从 %s 推导仓库名", gitURL)
				}
				org := strArg(args, "org")
				if org == "" {
					org = "external"
				}
				v, err := s.httpPostJSON(ctx, s.jj+"/api/v1/repos/"+urlPathEscape(org)+"/"+urlPathEscape(repo)+"/clone",
					map[string]interface{}{"url": gitURL})
				return extension.ToolResultData{Content: v}, err
			},
		},
	}
}

// jobArgs lifts the shared job params (id + output window) for job RPCs.
func jobArgs(args map[string]interface{}) map[string]interface{} {
	out := map[string]interface{}{
		"job_id": strArg(args, "job-id"),
	}
	if v := args["start"]; v != nil {
		out["start"] = v
	}
	if v := args["end"]; v != nil {
		out["end"] = v
	}
	if g := strArg(args, "grep"); g != "" {
		out["grep"] = g
	}
	if v := args["timeout-ms"]; v != nil {
		out["timeout_ms"] = v
	}
	return out
}

func strArg(args map[string]interface{}, k string) string {
	if v, ok := args[k].(string); ok {
		return v
	}
	return ""
}

// k8sServiceName derives a k8s-safe, globally-unique service name from the
// workspace triple (org-repo-bookmark), so deployments never collide across
// orgs/repos/sessions. Falls back to a hash of the raw session when the
// components exceed the 63-char label limit.
func k8sServiceName(org, repo, bm string) string {
	s := strings.ToLower(org + "-" + repo + "-" + bm)
	// Replace characters that are invalid in a DNS-1123 label.
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			b.WriteRune(r)
		case r == '.' || r == '_' || r == '/':
			b.WriteByte('-')
		default:
			b.WriteByte('-')
		}
	}
	name := strings.Trim(b.String(), "-")
	if len(name) <= 63 {
		return name
	}
	// Over-long: hash to stay within the label limit while remaining unique.
	h := sha256.Sum256([]byte(org + ":" + repo + ":" + bm))
	return fmt.Sprintf("svc-%x", h[:8])
}

// fetchService reads a single service's status from jjlab and returns its
// annotations (keyed flat) or nil when the service does not exist. It lets
// the deploy path verify ownership via `zergx/session` before mutating a
// deployment that may already exist.
func (s *server) fetchService(ctx context.Context, name, namespace string) (map[string]interface{}, error) {
	u := s.jj + "/api/v1/ops/services/" + url.PathEscape(name)
	if namespace != "" {
		u += "?namespace=" + url.QueryEscape(namespace)
	}
	raw, err := s.httpGetJSON(ctx, u)
	if err != nil {
		// 404 (not found) is the normal "no existing service" case.
		if strings.Contains(err.Error(), "404") {
			return nil, nil
		}
		return nil, err
	}
	var out map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	// Flatten annotations into the returned map under "session" for the
	// ownership check; callers only need zergx/session here.
	if ann, ok := out["annotations"].(map[string]interface{}); ok {
		if s, ok := ann["zergx/session"].(string); ok {
			out["session"] = s
		}
	}
	return out, nil
}

// qualifyImage converts a possibly-bare image reference into a fully-qualified in-cluster reference, mirroring how container-build tags
// images (artifactImageHost/{tag}:{bookmark}). It accepts:
//   - "example-server"          -> "jj-lab.temp.svc.cluster.local/example-server:<defaultTag>"
//   - "example-server:main"     -> "jj-lab.temp.svc.cluster.local/example-server:main"
//   - "repo/name:tag"           -> "jj-lab.temp.svc.cluster.local/repo/name:tag"
//   - "host/repo/name:tag"      -> left unchanged (already qualified)
//
// defaultTag is the tag appended when the reference is bare; it should match
// container-build's default (the session bookmark) so build+deploy agree.
func (s *server) qualifyImage(ref, defaultTag string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ref
	}
	host := s.artifactImageHost
	// A registry host is present only when the first path segment (before the
	// first '/') looks like a host: contains '.' or a colon followed by a
	// numeric port, or equals "localhost". A bare "name:tag" (e.g. "example-
	// server:main") has no '.' and its colon is followed by a non-numeric tag,
	// so it is NOT a host and must be qualified.
	first := ref
	if i := strings.IndexByte(ref, '/'); i >= 0 {
		first = ref[:i]
	}
	if isRegistryHost(first) {
		return ref
	}
	// Bare name, name:tag, or repo/name[:tag] — qualify with the artifact host.
	if !strings.Contains(ref, ":") {
		if defaultTag == "" {
			defaultTag = "latest"
		}
		ref += ":" + defaultTag
	}
	return host + "/" + ref
}

// isRegistryHost reports whether a first path segment is a registry host:
// contains a '.', is "localhost", or is "host:port" with a numeric port.
func isRegistryHost(seg string) bool {
	if seg == "localhost" {
		return true
	}
	if strings.Contains(seg, ".") {
		return true
	}
	if i := strings.IndexByte(seg, ':'); i >= 0 {
		port := seg[i+1:]
		if port != "" {
			if _, err := strconv.Atoi(port); err == nil {
				return true
			}
		}
	}
	return false
}

func intArg64(args map[string]interface{}, k string, def int64) int64 {
	if v, ok := args[k].(float64); ok {
		return int64(v)
	}
	return def
}

func boolArg(args map[string]interface{}, k string) bool {
	if v, ok := args[k].(bool); ok {
		return v
	}
	return false
}

// envMapFromArgs lifts a flat set of args into a string→string env map for the
// service-deploy service spec.
func envMapFromArgs(args map[string]interface{}) map[string]string {
	if raw, ok := args["env"].(map[string]interface{}); ok {
		out := map[string]string{}
		for k, v := range raw {
			if s, ok := v.(string); ok {
				out[k] = s
			}
		}
		return out
	}
	return nil
}

func toJSON(v interface{}) string {
	b, err := jsonMarshalIndent(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}

func jsonMarshalIndent(v interface{}) ([]byte, error) {
	return marshalIndent(v)
}

var _ = strings.TrimSpace

// sandboxEdit implements edit as read-modify-write over the worker sandbox
// using the native file_read/file_write RPCs (the minimal worker shell has no
// pipes/redirection).
func (s *server) sandboxEdit(ctx context.Context, sessionName, cid, path string, startLine, endLine int64, content string) (string, error) {
	data, err := s.sandboxFileRead(ctx, cid, path)
	if err != nil {
		return "", ef(ctx, s.ext, sessionName, "sandbox edit read failed: %v", "sandbox 编辑读取失败：%v", err)
	}
	current := string(data)
	lines := strings.Split(current, "\n")
	var newLines []string
	if content != "" {
		newLines = strings.Split(content, "\n")
	}
	if endLine < startLine {
		// insert before startLine (1-based)
		insertAt := int(startLine - 1)
		if insertAt > len(lines) {
			insertAt = len(lines)
		}
		v := append([]string{}, lines[:insertAt]...)
		v = append(v, newLines...)
		v = append(v, lines[insertAt:]...)
		lines = v
	} else {
		// replace [startLine, endLine] (1-based, inclusive)
		sIdx := int(startLine - 1)
		eIdx := int(endLine)
		if eIdx > len(lines) {
			eIdx = len(lines)
		}
		if sIdx > len(lines) {
			sIdx = len(lines)
		}
		v := append([]string{}, lines[:sIdx]...)
		v = append(v, newLines...)
		v = append(v, lines[eIdx:]...)
		lines = v
	}
	newContent := strings.Join(lines, "\n")
	if err := s.sandboxFileWrite(ctx, cid, path, []byte(newContent)); err != nil {
		return "", ef(ctx, s.ext, sessionName, "sandbox edit write failed: %v", "sandbox 编辑写入失败：%v", err)
	}
	return fmt.Sprintf("Edited sandbox file '%s'.", path), nil
}

// rawMap coerces the workerCommand result to map[string]interface{}.
func rawMap(v interface{}) map[string]interface{} {
	if m, ok := v.(map[string]interface{}); ok {
		return m
	}
	return map[string]interface{}{}
}

// portFile copies a file (or a whole directory, recursively) from the sandbox
// into the session's jj repo as ONE atomic change. The workspace bookmark is
// the destination bookmark.
//
//   - single file: reads via worker file_read, then PUTs the repo contents API
//     with an optimistic-lock base blob sha (a concurrent change is rejected).
//   - directory: walks it via worker file_list, reads repo blob sha per target
//     file, then POSTs `batch` (atomic, single change id).
//
// On success the change id is returned so the agent can track/review the edit.
func (s *server) portFile(ctx context.Context, sessionName string, sc sandboxCtx, args map[string]interface{}) (string, error) {
	sandboxPath := strArg(args, "sandbox-path")
	repoPath := strArg(args, "repo-path")
	message := strArg(args, "message")
	if message == "" {
		message = "port " + sandboxPath
	}

	commitsPath := fmt.Sprintf("%s/api/v1/repos/%s/%s/commits",
		s.jj, urlPathEscape(sc.ws.org), urlPathEscape(sc.ws.repo))

	// Determine whether sandbox_path is a directory.
	info, err := s.sandboxFileStat(ctx, sc.cid, sandboxPath)
	if err != nil {
		return "", ef(ctx, s.ext, sessionName, "port sandbox stat failed: %v", "沙箱 stat 失败：%v", err)
	}

	if !info.IsDir() {
		// Single file: read + optimistic-lock commit (one action).
		data, err := s.sandboxFileRead(ctx, sc.cid, sandboxPath)
		if err != nil {
			return "", ef(ctx, s.ext, sessionName, "port sandbox read failed: %v", "沙箱读取失败：%v", err)
		}
		sha, err := s.repoBlobSha(ctx, sc.ws.org, sc.ws.repo, repoPath, sc.ws.bookmark)
		if err != nil && !errors.Is(err, errNotFoundForHTTP) {
			return "", ef(ctx, s.ext, sessionName, "port read repo sha failed: %v", "读取仓库 sha 失败：%v", err)
		}
		action := map[string]interface{}{
			"action":         "update",
			"path":           repoPath,
			"content_base64": base64Encode(string(data)),
		}
		if sha != "" {
			action["sha"] = sha
		}
		var resp map[string]interface{}
		if err := s.httpPostJSONMap(ctx, commitsPath, map[string]interface{}{
			"bookmark": sc.ws.bookmark,
			"message":  message,
			"actions":  []interface{}{action},
		}, &resp); err != nil {
			return "", ef(ctx, s.ext, sessionName, "port write failed: %v", "沙箱写入失败：%v", err)
		}
		changeID := strField(resp, "change_id")
		if changeID == "" {
			changeID = strField(resp, "commit_id")
		}
		return fmt.Sprintf("Ported '%s' to repo '%s' (change %s).", sandboxPath, repoPath, shortID(changeID)), nil
	}

	// Directory: expand via worker file_list, then one atomic commit (multi-action).
	files, err := s.sandboxFileList(ctx, sc.cid, sandboxPath)
	if err != nil {
		return "", ef(ctx, s.ext, sessionName, "port sandbox list failed: %v", "沙箱列表失败：%v", err)
	}
	if len(files) == 0 {
		return "", ef(ctx, s.ext, sessionName, "port sandbox directory '%s' is empty", "沙箱目录 '%s' 为空", sandboxPath)
	}
	actions := make([]map[string]interface{}, 0, len(files))
	for _, f := range files {
		rel := f["path"].(string)
		target := repoPath
		if target == "" || strings.HasSuffix(target, "/") {
			target = strings.TrimRight(repoPath, "/") + "/" + rel
		} else {
			target = target + "/" + rel
		}
		contentBase64, _ := f["content"].(string)
		sha, serr := s.repoBlobSha(ctx, sc.ws.org, sc.ws.repo, target, sc.ws.bookmark)
		if serr != nil && !errors.Is(serr, errNotFoundForHTTP) {
			return "", ef(ctx, s.ext, sessionName, "port read repo sha failed: %v", "读取仓库 sha 失败：%v", serr)
		}
		action := map[string]interface{}{"action": "update", "path": target, "content_base64": contentBase64}
		if sha != "" {
			action["sha"] = sha
		}
		actions = append(actions, action)
	}
	var resp map[string]interface{}
	if err := s.httpPostJSONMap(ctx, commitsPath, map[string]interface{}{
		"bookmark": sc.ws.bookmark,
		"message":  message,
		"actions":  actions,
	}, &resp); err != nil {
		return "", ef(ctx, s.ext, sessionName, "port commit write failed: %v", "提交写入失败：%v", err)
	}
	changeID := strField(resp, "change_id")
	if changeID == "" {
		changeID = strField(resp, "commit_id")
	}
	return fmt.Sprintf("Ported directory '%s' to repo '%s' (%d file(s), change %s).", sandboxPath, repoPath, len(actions), shortID(changeID)), nil
}

// repoBlobSha returns the current blob sha for a path at a ref, or "" when the
// file does not exist yet (so a create may proceed without a base).
func (s *server) repoBlobSha(ctx context.Context, org, repo, path, ref string) (string, error) {
	url := fmt.Sprintf("%s/api/v1/repos/%s/%s/contents/%s?ref=%s",
		s.jj, urlPathEscape(org), urlPathEscape(repo), escapePath(path), urlPathEscape(ref))
	var v map[string]interface{}
	if err := s.httpGetJSONMap(ctx, url, &v); err != nil {
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "not found") {
			return "", errNotFoundForHTTP
		}
		return "", err
	}
	if s, ok := v["sha"].(string); ok {
		return s, nil
	}
	return "", nil
}

// imageList queries the artifact registry's OCI catalog (GET /v2/_catalog)
// and returns the repository names.
func (s *server) imageList(ctx context.Context) (string, error) {
	return s.httpGetJSON(ctx, s.artifact+"/v2/_catalog")
}

// resourceRequestFromArgs extracts the nested resources arg (if any) from a
// tool argument map, tolerant of missing/malformed entries.
func resourceRequestFromArgs(args map[string]interface{}) *ResourceRequest {
	rr := &ResourceRequest{}
	raw, ok := args["resources"].(map[string]interface{})
	if !ok {
		return rr
	}
	if reqs, ok := raw["requests"].(map[string]interface{}); ok {
		rr.Requests = &ResourcePair{
			CPU:    strArg(reqs, "cpu"),
			Memory: strArg(reqs, "memory"),
		}
	}
	if limits, ok := raw["limits"].(map[string]interface{}); ok {
		rr.Limits = &ResourcePair{
			CPU:    strArg(limits, "cpu"),
			Memory: strArg(limits, "memory"),
		}
	}
	return rr
}

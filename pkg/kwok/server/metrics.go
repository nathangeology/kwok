/*
Copyright 2023 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/emicklei/go-restful/v3"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"sigs.k8s.io/kwok/pkg/apis/internalversion"
	"sigs.k8s.io/kwok/pkg/config/resources"
	"sigs.k8s.io/kwok/pkg/kwok/metrics"
	"sigs.k8s.io/kwok/pkg/log"
)

func (s *Server) initCEL() error {
	if s.env != nil {
		return fmt.Errorf("CEL environment already initialized")
	}

	env, err := metrics.NewEnvironment(metrics.EnvironmentConfig{
		EnableResultCache:      true,
		StartedContainersTotal: s.dataSource.StartedContainersTotal,

		ContainerResourceUsage: s.containerResourceUsage,
		PodResourceUsage:       s.podResourceUsage,
		NodeResourceUsage:      s.nodeResourceUsage,

		ContainerResourceCumulativeUsage: s.containerResourceCumulativeUsage,
		PodResourceCumulativeUsage:       s.podResourceCumulativeUsage,
		NodeResourceCumulativeUsage:      s.nodeResourceCumulativeUsage,
	})
	if err != nil {
		return fmt.Errorf("failed to create CEL environment: %w", err)
	}
	s.env = env
	return nil
}

// InstallMetrics registers the metrics handler on the given mux.
func (s *Server) InstallMetrics(ctx context.Context) error {
	err := s.initCEL()
	if err != nil {
		return err
	}

	promHandler := promhttp.Handler()

	selfMetric := func(req *restful.Request, resp *restful.Response) {
		promHandler.ServeHTTP(resp.ResponseWriter, req.Request)
	}

	const rootPath = "/metrics"
	ws := new(restful.WebService)
	ws.Path(rootPath)
	ws.Route(ws.GET("/").To(selfMetric))
	s.restfulCont.Add(ws)

	syncd, ok := s.metrics.(resources.Synced)
	if ok {
		go s.dynamicMetricsPath(ctx, ws, syncd, rootPath)
	} else {
		for _, m := range s.metrics.Get() {
			if !strings.HasPrefix(m.Spec.Path, rootPath) {
				return fmt.Errorf("metric path %q does not start with %q", m.Spec.Path, rootPath)
			}
			path := strings.TrimPrefix(m.Spec.Path, rootPath)
			ws.Route(ws.GET(path).To(s.getMetrics(m, s.env)))

			// Register a flat route for metrics-server compatibility.
			// e.g. /nodes/{nodeName}/metrics/resource → /resource
			if flat, ok := flatMetricsPath(path); ok {
				ws.Route(ws.GET(flat).To(s.getMetrics(m, s.env)))
			}
		}
	}

	return nil
}

func (s *Server) dynamicMetricsPath(ctx context.Context, ws *restful.WebService, syncd resources.Synced, rootPath string) {
	logger := log.FromContext(ctx)
	hasPaths := map[string]struct{}{}
	for range syncd.Sync() {
		newHasPaths := map[string]struct{}{}
		for _, m := range s.metrics.Get() {
			if !strings.HasPrefix(m.Spec.Path, rootPath) {
				logger.Warn("metric path does not start with "+rootPath, "path", m.Spec.Path)
				continue
			}

			path := strings.TrimPrefix(m.Spec.Path, rootPath)
			handler := s.getMetrics(m, s.env)

			// Collect all paths to register for this metric.
			paths := []string{path}
			if flat, ok := flatMetricsPath(path); ok {
				paths = append(paths, flat)
			}

			for _, p := range paths {
				newHasPaths[p] = struct{}{}
				if _, ok := hasPaths[p]; ok {
					err := ws.RemoveRoute(http.MethodGet, p)
					if err != nil {
						logger.Error("Failed to remove route", err, "path", p)
					}
				}
				ws.Route(ws.GET(p).To(handler))
			}
		}

		for path := range hasPaths {
			if _, ok := newHasPaths[path]; !ok {
				err := ws.RemoveRoute(http.MethodGet, path)
				if err != nil {
					logger.Error("Failed to remove route", err, "path", path)
				}
			}
		}

		hasPaths = newHasPaths
	}
}

// resolveNodeName attempts to determine the target node name from the request.
// It checks the Host header against known node names and addresses.
func (s *Server) resolveNodeName(req *http.Request) string {
	host := req.Host
	// Strip port if present.
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	// Check if the host matches a known node name directly.
	if _, ok := s.nodeCacheGetter.Get(host); ok {
		return host
	}

	// Check if the host IP matches a node's address.
	for _, nodeName := range s.dataSource.ListNodes() {
		node, ok := s.nodeCacheGetter.Get(nodeName)
		if !ok {
			continue
		}
		for _, addr := range node.Status.Addresses {
			if addr.Address == host {
				return nodeName
			}
		}
	}

	return ""
}

// flatMetricsPath returns the path with /nodes/{nodeName} segments removed,
// e.g. "/nodes/{nodeName}/metrics/resource" becomes "/resource".
func flatMetricsPath(path string) (string, bool) {
	const segment = "/nodes/{nodeName}"
	idx := strings.Index(path, segment)
	if idx < 0 {
		return "", false
	}
	return path[:idx] + path[idx+len(segment):], true
}

func (s *Server) getMetrics(metric *internalversion.Metric, env *metrics.Environment) func(req *restful.Request, resp *restful.Response) {
	return func(req *restful.Request, resp *restful.Response) {
		nodeName := req.PathParameter("nodeName")
		if nodeName == "" {
			nodeName = s.resolveNodeName(req.Request)
		}
		if nodeName == "" {
			// Fallback: use the first node if only one exists.
			nodes := s.dataSource.ListNodes()
			if len(nodes) == 1 {
				nodeName = nodes[0]
			}
		}
		if nodeName == "" {
			resp.WriteHeader(http.StatusNotFound)
			return
		}

		handler, ok := s.metricsUpdateHandler.Load(nodeName)
		if !ok {
			handler = metrics.NewMetricsUpdateHandler(metrics.UpdateHandlerConfig{
				Environment:     env,
				DataSource:      s.dataSource,
				NodeCacheGetter: s.nodeCacheGetter,
				PodCacheGetter:  s.podCacheGetter,
			})
			s.metricsUpdateHandler.Store(nodeName, handler)
		}

		handler.Update(req.Request.Context(), nodeName, metric.Spec.Metrics)
		handler.ServeHTTP(resp.ResponseWriter, req.Request)
	}
}

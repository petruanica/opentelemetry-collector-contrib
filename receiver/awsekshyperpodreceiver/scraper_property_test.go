// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awsekshyperpodreceiver

import (
	"strings"
	"testing"

	"pgregory.net/rapid"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/k8s/k8sclient"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/k8s/k8sutil"
)

// --- Generators ---

// genNodeName generates random node names, some with "hyperpod-" prefix, some without.
func genNodeName() *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		base := rapid.StringMatching(`[a-z][a-z0-9\-]{2,20}`).Draw(t, "base")
		if rapid.Bool().Draw(t, "has_prefix") {
			return "hyperpod-" + base
		}
		return base
	})
}

// genHealthStatus generates a valid health status int8 (0-3).
func genHealthStatus() *rapid.Generator[int8] {
	return rapid.Custom(func(t *rapid.T) int8 {
		return int8(rapid.IntRange(0, 3).Draw(t, "status"))
	})
}

// genInstanceType generates instance types, some with "ml." prefix, some without.
func genInstanceType() *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		if rapid.Bool().Draw(t, "is_ml") {
			suffix := rapid.SampledFrom([]string{"p4d.24xlarge", "g5.xlarge", "trn1.32xlarge", "p5.48xlarge"}).Draw(t, "ml_suffix")
			return "ml." + suffix
		}
		suffix := rapid.SampledFrom([]string{"m5.xlarge", "c5.2xlarge", "r5.large", "t3.medium"}).Draw(t, "non_ml_suffix")
		return suffix
	})
}

// genClusterName generates empty and non-empty cluster names.
func genClusterName() *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		if rapid.Bool().Draw(t, "has_cluster") {
			return rapid.StringMatching(`[a-z][a-z0-9\-]{2,15}`).Draw(t, "cluster")
		}
		return ""
	})
}

// --- Property 1: Metric emission correctness ---

func TestProperty_MetricEmissionCorrectness(t *testing.T) {
	ctx := t.Context()
	rapid.Check(t, func(t *rapid.T) {
		// Generate 1-10 nodes, each with a valid health status.
		nodeCount := rapid.IntRange(1, 10).Draw(t, "node_count")

		nodeInfos := make(map[string]*k8sclient.NodeInfo)
		nodeToLabelsMap := make(map[string]map[k8sclient.Label]int8)
		nodeStatuses := make(map[string]int8)

		for i := 0; i < nodeCount; i++ {
			name := genNodeName().Draw(t, "node_name")
			// Ensure unique node names by appending index.
			name = name + "-" + rapid.StringMatching(`[0-9]{4}`).Draw(t, "suffix")
			status := genHealthStatus().Draw(t, "status")
			instanceType := genInstanceType().Draw(t, "instance_type")

			nodeInfos[name] = &k8sclient.NodeInfo{
				Name:         name,
				InstanceType: instanceType,
			}
			nodeToLabelsMap[name] = map[k8sclient.Label]int8{
				k8sclient.SageMakerNodeHealthStatus: status,
			}
			nodeStatuses[name] = status
		}

		cfg := defaultTestConfig()
		s := newTestScraper(cfg)
		s.nodeClient = &mockNodeClient{
			nodeInfos:       nodeInfos,
			nodeToLabelsMap: nodeToLabelsMap,
		}

		metrics, err := s.scrape(ctx)
		if err != nil {
			t.Fatalf("scrape returned error: %v", err)
		}

		sm := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0)

		// Should emit exactly 4 metrics per node.
		expectedMetricCount := len(nodeStatuses) * 4
		if sm.Metrics().Len() != expectedMetricCount {
			t.Fatalf("expected %d metrics, got %d", expectedMetricCount, sm.Metrics().Len())
		}

		// Group metrics by node_name.
		type metricEntry struct {
			name  string
			value int64
		}
		nodeMetrics := make(map[string][]metricEntry)
		for i := 0; i < sm.Metrics().Len(); i++ {
			m := sm.Metrics().At(i)
			dp := m.Gauge().DataPoints().At(0)
			nodeName, _ := dp.Attributes().Get("node_name")
			nodeMetrics[nodeName.Str()] = append(nodeMetrics[nodeName.Str()], metricEntry{
				name:  m.Name(),
				value: dp.IntValue(),
			})
		}

		// Verify each node has exactly 4 metrics, exactly one = 1, rest = 0.
		for nodeName, entries := range nodeMetrics {
			if len(entries) != 4 {
				t.Fatalf("node %s: expected 4 metrics, got %d", nodeName, len(entries))
			}

			onesCount := 0
			zerosCount := 0
			for _, e := range entries {
				switch e.value {
				case 1:
					onesCount++
				case 0:
					zerosCount++
				default:
					t.Fatalf("node %s: unexpected metric value %d for %s", nodeName, e.value, e.name)
				}
			}
			if onesCount != 1 {
				t.Fatalf("node %s: expected exactly 1 metric with value 1, got %d", nodeName, onesCount)
			}
			if zerosCount != 3 {
				t.Fatalf("node %s: expected exactly 3 metrics with value 0, got %d", nodeName, zerosCount)
			}

			// Verify the metric set to 1 matches the node's status.
			expectedStatus := k8sutil.HyperPodConditionType(nodeStatuses[nodeName]).String()
			expectedMetricName := statusToMetricName[expectedStatus]
			found := false
			for _, e := range entries {
				if e.value == 1 && e.name == expectedMetricName {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("node %s: expected metric %s to be 1, but it wasn't", nodeName, expectedMetricName)
			}
		}
	})
}

// --- Property 2: Attribute correctness ---

func TestProperty_AttributeCorrectness(t *testing.T) {
	ctx := t.Context()
	rapid.Check(t, func(t *rapid.T) {
		nodeName := genNodeName().Draw(t, "node_name")
		status := genHealthStatus().Draw(t, "status")
		clusterName := genClusterName().Draw(t, "cluster_name")
		instanceType := genInstanceType().Draw(t, "instance_type")

		cfg := defaultTestConfig()
		cfg.ClusterName = clusterName
		s := newTestScraper(cfg)
		s.nodeClient = &mockNodeClient{
			nodeInfos: map[string]*k8sclient.NodeInfo{
				nodeName: {Name: nodeName, InstanceType: instanceType},
			},
			nodeToLabelsMap: map[string]map[k8sclient.Label]int8{
				nodeName: {k8sclient.SageMakerNodeHealthStatus: status},
			},
		}

		metrics, err := s.scrape(ctx)
		if err != nil {
			t.Fatalf("scrape returned error: %v", err)
		}

		sm := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0)
		if sm.Metrics().Len() != 4 {
			t.Fatalf("expected 4 metrics, got %d", sm.Metrics().Len())
		}

		// Expected instance_id: strip "hyperpod-" prefix if present.
		expectedInstanceID := strings.TrimPrefix(nodeName, "hyperpod-")

		for i := 0; i < sm.Metrics().Len(); i++ {
			dp := sm.Metrics().At(i).Gauge().DataPoints().At(0)
			attrs := dp.Attributes()

			// Verify node_name.
			nn, ok := attrs.Get("node_name")
			if !ok || nn.Str() != nodeName {
				t.Fatalf("expected node_name=%q, got %q (ok=%v)", nodeName, nn.Str(), ok)
			}

			// Verify instance_id.
			iid, ok := attrs.Get("instance_id")
			if !ok || iid.Str() != expectedInstanceID {
				t.Fatalf("expected instance_id=%q, got %q (ok=%v)", expectedInstanceID, iid.Str(), ok)
			}

			// Verify cluster_name presence/absence.
			cn, hasCN := attrs.Get("cluster_name")
			if clusterName != "" {
				if !hasCN || cn.Str() != clusterName {
					t.Fatalf("expected cluster_name=%q, got %q (present=%v)", clusterName, cn.Str(), hasCN)
				}
			} else {
				if hasCN {
					t.Fatalf("expected cluster_name to be absent when config is empty, but got %q", cn.Str())
				}
			}
		}
	})
}

// --- Property 4: Any instance type with health label emits metrics ---

func TestProperty_HealthLabelDeterminesEmission(t *testing.T) {
	ctx := t.Context()
	rapid.Check(t, func(t *rapid.T) {
		nodeName := genNodeName().Draw(t, "node_name")
		instanceType := genInstanceType().Draw(t, "instance_type")
		hasHealthLabel := rapid.Bool().Draw(t, "has_health_label")
		status := genHealthStatus().Draw(t, "status")

		cfg := defaultTestConfig()
		s := newTestScraper(cfg)

		nodeInfos := map[string]*k8sclient.NodeInfo{
			nodeName: {Name: nodeName, InstanceType: instanceType},
		}
		nodeToLabelsMap := make(map[string]map[k8sclient.Label]int8)
		if hasHealthLabel {
			nodeToLabelsMap[nodeName] = map[k8sclient.Label]int8{
				k8sclient.SageMakerNodeHealthStatus: status,
			}
		}

		s.nodeClient = &mockNodeClient{
			nodeInfos:       nodeInfos,
			nodeToLabelsMap: nodeToLabelsMap,
		}

		metrics, err := s.scrape(ctx)
		if err != nil {
			t.Fatalf("scrape returned error: %v", err)
		}

		sm := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0)
		metricsEmitted := sm.Metrics().Len() > 0

		// Metrics emitted iff node has a valid health status label.
		shouldEmit := hasHealthLabel

		if metricsEmitted != shouldEmit {
			t.Fatalf("node=%q instanceType=%q hasLabel=%v: expected emit=%v, got emit=%v (metrics=%d)",
				nodeName, instanceType, hasHealthLabel, shouldEmit, metricsEmitted, sm.Metrics().Len())
		}

		// If metrics were emitted, verify count is exactly 4.
		if metricsEmitted && sm.Metrics().Len() != 4 {
			t.Fatalf("expected 4 metrics when emitting, got %d", sm.Metrics().Len())
		}
	})
}

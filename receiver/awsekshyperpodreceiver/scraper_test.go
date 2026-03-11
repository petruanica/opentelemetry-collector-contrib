// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awsekshyperpodreceiver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
	v1 "k8s.io/api/core/v1"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/k8s/k8sclient"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/k8s/k8sutil"
)

// mockNodeClient implements k8sclient.NodeClient for testing.
type mockNodeClient struct {
	nodeInfos       map[string]*k8sclient.NodeInfo
	nodeToLabelsMap map[string]map[k8sclient.Label]int8
}

func (m *mockNodeClient) NodeInfos() map[string]*k8sclient.NodeInfo {
	return m.nodeInfos
}

func (m *mockNodeClient) ClusterFailedNodeCount() int { return 0 }
func (m *mockNodeClient) ClusterNodeCount() int       { return 0 }

func (m *mockNodeClient) NodeToCapacityMap() map[string]v1.ResourceList    { return nil }
func (m *mockNodeClient) NodeToAllocatableMap() map[string]v1.ResourceList { return nil }
func (m *mockNodeClient) NodeToConditionsMap() map[string]map[v1.NodeConditionType]v1.ConditionStatus {
	return nil
}

func (m *mockNodeClient) NodeToLabelsMap() map[string]map[k8sclient.Label]int8 {
	return m.nodeToLabelsMap
}

func newTestScraper(cfg *Config) *scraper {
	settings := receivertest.NewNopSettings(component.MustNewType("awsekshyperpodreceiver"))
	return newScraper(cfg, settings)
}

func defaultTestConfig() *Config {
	cfg := &Config{
		ControllerConfig: scraperhelper.NewDefaultControllerConfig(),
		ClusterName:      "test-cluster",
	}
	cfg.CollectionInterval = 60 * time.Second
	return cfg
}

// --- isValidHealthStatus tests ---

func TestIsValidHealthStatus(t *testing.T) {
	validStatuses := []int8{0, 1, 2, 3}
	for _, s := range validStatuses {
		assert.True(t, isValidHealthStatus(s), "expected %d to be valid", s)
	}

	invalidStatuses := []int8{-1, 4, 99, -128, 127}
	for _, s := range invalidStatuses {
		assert.False(t, isValidHealthStatus(s), "expected %d to be invalid", s)
	}
}

// --- statusToMetricName tests ---

func TestStatusToMetricName(t *testing.T) {
	tests := []struct {
		status   int8
		expected string
	}{
		{int8(k8sutil.Schedulable), "hyperpod_node_health_status_schedulable"},
		{int8(k8sutil.UnschedulablePendingReplacement), "hyperpod_node_health_status_unschedulable_pending_replacement"},
		{int8(k8sutil.UnschedulablePendingReboot), "hyperpod_node_health_status_unschedulable_pending_reboot"},
		{int8(k8sutil.Unschedulable), "hyperpod_node_health_status_unschedulable"},
	}
	for _, tt := range tests {
		statusStr := k8sutil.HyperPodConditionType(tt.status).String()
		t.Run(statusStr, func(t *testing.T) {
			assert.Equal(t, tt.expected, statusToMetricName[statusStr])
		})
	}
}

// --- Health status metric value tests ---

func TestScrape_SchedulableStatus(t *testing.T) {
	s := newTestScraper(defaultTestConfig())
	s.nodeClient = &mockNodeClient{
		nodeInfos: map[string]*k8sclient.NodeInfo{
			"node-1": {Name: "node-1", InstanceType: "ml.p4d.24xlarge"},
		},
		nodeToLabelsMap: map[string]map[k8sclient.Label]int8{
			"node-1": {k8sclient.SageMakerNodeHealthStatus: int8(k8sutil.Schedulable)},
		},
	}

	metrics, err := s.scrape(t.Context())
	require.NoError(t, err)

	sm := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0)
	require.Equal(t, 4, sm.Metrics().Len())

	expected := map[string]int64{
		"hyperpod_node_health_status_schedulable":                       1,
		"hyperpod_node_health_status_unschedulable_pending_replacement": 0,
		"hyperpod_node_health_status_unschedulable_pending_reboot":      0,
		"hyperpod_node_health_status_unschedulable":                     0,
	}
	for i := 0; i < sm.Metrics().Len(); i++ {
		m := sm.Metrics().At(i)
		dp := m.Gauge().DataPoints().At(0)
		assert.Equal(t, expected[m.Name()], dp.IntValue(), "metric %s", m.Name())
	}
}

func TestScrape_UnschedulableStatus(t *testing.T) {
	s := newTestScraper(defaultTestConfig())
	s.nodeClient = &mockNodeClient{
		nodeInfos: map[string]*k8sclient.NodeInfo{
			"node-1": {Name: "node-1", InstanceType: "ml.p4d.24xlarge"},
		},
		nodeToLabelsMap: map[string]map[k8sclient.Label]int8{
			"node-1": {k8sclient.SageMakerNodeHealthStatus: int8(k8sutil.Unschedulable)},
		},
	}

	metrics, err := s.scrape(t.Context())
	require.NoError(t, err)

	sm := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0)
	require.Equal(t, 4, sm.Metrics().Len())

	for i := 0; i < sm.Metrics().Len(); i++ {
		m := sm.Metrics().At(i)
		dp := m.Gauge().DataPoints().At(0)
		if m.Name() == "hyperpod_node_health_status_unschedulable" {
			assert.Equal(t, int64(1), dp.IntValue())
		} else {
			assert.Equal(t, int64(0), dp.IntValue(), "metric %s should be 0", m.Name())
		}
	}
}

func TestScrape_UnschedulablePendingReplacementStatus(t *testing.T) {
	s := newTestScraper(defaultTestConfig())
	s.nodeClient = &mockNodeClient{
		nodeInfos: map[string]*k8sclient.NodeInfo{
			"node-1": {Name: "node-1", InstanceType: "ml.p4d.24xlarge"},
		},
		nodeToLabelsMap: map[string]map[k8sclient.Label]int8{
			"node-1": {k8sclient.SageMakerNodeHealthStatus: int8(k8sutil.UnschedulablePendingReplacement)},
		},
	}

	metrics, err := s.scrape(t.Context())
	require.NoError(t, err)

	sm := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0)
	require.Equal(t, 4, sm.Metrics().Len())

	for i := 0; i < sm.Metrics().Len(); i++ {
		m := sm.Metrics().At(i)
		dp := m.Gauge().DataPoints().At(0)
		if m.Name() == "hyperpod_node_health_status_unschedulable_pending_replacement" {
			assert.Equal(t, int64(1), dp.IntValue())
		} else {
			assert.Equal(t, int64(0), dp.IntValue(), "metric %s should be 0", m.Name())
		}
	}
}

func TestScrape_UnschedulablePendingRebootStatus(t *testing.T) {
	s := newTestScraper(defaultTestConfig())
	s.nodeClient = &mockNodeClient{
		nodeInfos: map[string]*k8sclient.NodeInfo{
			"node-1": {Name: "node-1", InstanceType: "ml.p4d.24xlarge"},
		},
		nodeToLabelsMap: map[string]map[k8sclient.Label]int8{
			"node-1": {k8sclient.SageMakerNodeHealthStatus: int8(k8sutil.UnschedulablePendingReboot)},
		},
	}

	metrics, err := s.scrape(t.Context())
	require.NoError(t, err)

	sm := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0)
	require.Equal(t, 4, sm.Metrics().Len())

	for i := 0; i < sm.Metrics().Len(); i++ {
		m := sm.Metrics().At(i)
		dp := m.Gauge().DataPoints().At(0)
		if m.Name() == "hyperpod_node_health_status_unschedulable_pending_reboot" {
			assert.Equal(t, int64(1), dp.IntValue())
		} else {
			assert.Equal(t, int64(0), dp.IntValue(), "metric %s should be 0", m.Name())
		}
	}
}

// --- Nodes without health label are skipped ---

func TestScrape_NodeWithoutHealthLabel(t *testing.T) {
	s := newTestScraper(defaultTestConfig())
	s.nodeClient = &mockNodeClient{
		nodeInfos: map[string]*k8sclient.NodeInfo{
			"node-1": {Name: "node-1", InstanceType: "ml.p4d.24xlarge"},
		},
		// node-1 is NOT in nodeToLabelsMap, so it should be skipped
		nodeToLabelsMap: map[string]map[k8sclient.Label]int8{},
	}

	metrics, err := s.scrape(t.Context())
	require.NoError(t, err)

	sm := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0)
	assert.Equal(t, 0, sm.Metrics().Len())
}

// --- Invalid health status values are skipped ---

func TestScrape_InvalidHealthStatus(t *testing.T) {
	s := newTestScraper(defaultTestConfig())
	s.nodeClient = &mockNodeClient{
		nodeInfos: map[string]*k8sclient.NodeInfo{
			"node-1": {Name: "node-1", InstanceType: "ml.p4d.24xlarge"},
		},
		nodeToLabelsMap: map[string]map[k8sclient.Label]int8{
			"node-1": {k8sclient.SageMakerNodeHealthStatus: int8(99)},
		},
	}

	metrics, err := s.scrape(t.Context())
	require.NoError(t, err)

	sm := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0)
	assert.Equal(t, 0, sm.Metrics().Len(), "invalid health status should be skipped")
}

// --- HyperPod prefix stripping tests ---

func TestScrape_HyperPodPrefixRemoval(t *testing.T) {
	s := newTestScraper(defaultTestConfig())
	s.nodeClient = &mockNodeClient{
		nodeInfos: map[string]*k8sclient.NodeInfo{
			"hyperpod-i-1234567890abcdef0": {Name: "hyperpod-i-1234567890abcdef0", InstanceType: "ml.p4d.24xlarge"},
		},
		nodeToLabelsMap: map[string]map[k8sclient.Label]int8{
			"hyperpod-i-1234567890abcdef0": {k8sclient.SageMakerNodeHealthStatus: int8(k8sutil.Schedulable)},
		},
	}

	metrics, err := s.scrape(t.Context())
	require.NoError(t, err)

	sm := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0)
	require.Equal(t, 4, sm.Metrics().Len())

	dp := sm.Metrics().At(0).Gauge().DataPoints().At(0)
	instanceID, ok := dp.Attributes().Get("instance_id")
	require.True(t, ok)
	assert.Equal(t, "i-1234567890abcdef0", instanceID.Str())
}

func TestScrape_NoPrefixNodeName(t *testing.T) {
	s := newTestScraper(defaultTestConfig())
	s.nodeClient = &mockNodeClient{
		nodeInfos: map[string]*k8sclient.NodeInfo{
			"i-abcdef1234567890": {Name: "i-abcdef1234567890", InstanceType: "ml.p4d.24xlarge"},
		},
		nodeToLabelsMap: map[string]map[k8sclient.Label]int8{
			"i-abcdef1234567890": {k8sclient.SageMakerNodeHealthStatus: int8(k8sutil.Schedulable)},
		},
	}

	metrics, err := s.scrape(t.Context())
	require.NoError(t, err)

	sm := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0)
	require.Equal(t, 4, sm.Metrics().Len())

	dp := sm.Metrics().At(0).Gauge().DataPoints().At(0)
	instanceID, ok := dp.Attributes().Get("instance_id")
	require.True(t, ok)
	assert.Equal(t, "i-abcdef1234567890", instanceID.Str(), "instance_id should equal node name when no hyperpod- prefix")
}

// --- Cluster name attribute tests ---

func TestScrape_ClusterNamePresent(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.ClusterName = "my-cluster"
	s := newTestScraper(cfg)
	s.nodeClient = &mockNodeClient{
		nodeInfos: map[string]*k8sclient.NodeInfo{
			"node-1": {Name: "node-1", InstanceType: "ml.p4d.24xlarge"},
		},
		nodeToLabelsMap: map[string]map[k8sclient.Label]int8{
			"node-1": {k8sclient.SageMakerNodeHealthStatus: int8(k8sutil.Schedulable)},
		},
	}

	metrics, err := s.scrape(t.Context())
	require.NoError(t, err)

	sm := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0)
	require.Equal(t, 4, sm.Metrics().Len())

	for i := 0; i < sm.Metrics().Len(); i++ {
		dp := sm.Metrics().At(i).Gauge().DataPoints().At(0)
		clusterName, ok := dp.Attributes().Get("cluster_name")
		require.True(t, ok, "cluster_name should be present")
		assert.Equal(t, "my-cluster", clusterName.Str())
	}
}

func TestScrape_ClusterNameEmpty(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.ClusterName = ""
	s := newTestScraper(cfg)
	s.nodeClient = &mockNodeClient{
		nodeInfos: map[string]*k8sclient.NodeInfo{
			"node-1": {Name: "node-1", InstanceType: "ml.p4d.24xlarge"},
		},
		nodeToLabelsMap: map[string]map[k8sclient.Label]int8{
			"node-1": {k8sclient.SageMakerNodeHealthStatus: int8(k8sutil.Schedulable)},
		},
	}

	metrics, err := s.scrape(t.Context())
	require.NoError(t, err)

	sm := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0)
	require.Equal(t, 4, sm.Metrics().Len())

	dp := sm.Metrics().At(0).Gauge().DataPoints().At(0)
	_, ok := dp.Attributes().Get("cluster_name")
	assert.False(t, ok, "cluster_name should not be present when config is empty")
}

// --- Empty node list ---

func TestScrape_EmptyNodeList(t *testing.T) {
	s := newTestScraper(defaultTestConfig())
	s.nodeClient = &mockNodeClient{
		nodeInfos:       map[string]*k8sclient.NodeInfo{},
		nodeToLabelsMap: map[string]map[k8sclient.Label]int8{},
	}

	metrics, err := s.scrape(t.Context())
	require.NoError(t, err)

	sm := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0)
	assert.Equal(t, 0, sm.Metrics().Len())
}

// --- Shutdown with nil client ---

func TestShutdown_NilClient(t *testing.T) {
	s := newTestScraper(defaultTestConfig())
	// k8sClient is nil by default
	assert.Nil(t, s.k8sClient)

	// Should not panic
	err := s.shutdown(t.Context())
	assert.NoError(t, err)
}

// --- Multiple nodes test ---

func TestScrape_MultipleNodes(t *testing.T) {
	s := newTestScraper(defaultTestConfig())
	s.nodeClient = &mockNodeClient{
		nodeInfos: map[string]*k8sclient.NodeInfo{
			"hyperpod-i-111": {Name: "hyperpod-i-111", InstanceType: "ml.p4d.24xlarge"},
			"hyperpod-i-222": {Name: "hyperpod-i-222", InstanceType: "ml.p4d.24xlarge"},
		},
		nodeToLabelsMap: map[string]map[k8sclient.Label]int8{
			"hyperpod-i-111": {k8sclient.SageMakerNodeHealthStatus: int8(k8sutil.Schedulable)},
			"hyperpod-i-222": {k8sclient.SageMakerNodeHealthStatus: int8(k8sutil.Unschedulable)},
		},
	}

	metrics, err := s.scrape(t.Context())
	require.NoError(t, err)

	// 2 nodes × 4 metrics each = 8 metrics
	sm := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0)
	assert.Equal(t, 8, sm.Metrics().Len())
}

// --- Mixed nodes (valid, no label, invalid status) ---

func TestScrape_MixedNodes(t *testing.T) {
	s := newTestScraper(defaultTestConfig())
	s.nodeClient = &mockNodeClient{
		nodeInfos: map[string]*k8sclient.NodeInfo{
			"node-valid":          {Name: "node-valid", InstanceType: "ml.p4d.24xlarge"},
			"node-no-label":       {Name: "node-no-label", InstanceType: "ml.p4d.24xlarge"},
			"node-invalid-status": {Name: "node-invalid-status", InstanceType: "ml.p4d.24xlarge"},
		},
		nodeToLabelsMap: map[string]map[k8sclient.Label]int8{
			"node-valid":          {k8sclient.SageMakerNodeHealthStatus: int8(k8sutil.Schedulable)},
			"node-invalid-status": {k8sclient.SageMakerNodeHealthStatus: int8(99)},
			// node-no-label is not in nodeToLabelsMap
		},
	}

	metrics, err := s.scrape(t.Context())
	require.NoError(t, err)

	// Only node-valid should produce metrics (4 metrics)
	sm := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0)
	assert.Equal(t, 4, sm.Metrics().Len())
}

// --- Attributes correctness test ---

func TestScrape_AttributesCorrectness(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.ClusterName = "test-cluster"
	s := newTestScraper(cfg)
	s.nodeClient = &mockNodeClient{
		nodeInfos: map[string]*k8sclient.NodeInfo{
			"my-node": {Name: "my-node", InstanceType: "ml.p4d.24xlarge"},
		},
		nodeToLabelsMap: map[string]map[k8sclient.Label]int8{
			"my-node": {k8sclient.SageMakerNodeHealthStatus: int8(k8sutil.Schedulable)},
		},
	}

	metrics, err := s.scrape(t.Context())
	require.NoError(t, err)

	sm := metrics.ResourceMetrics().At(0).ScopeMetrics().At(0)
	require.Equal(t, 4, sm.Metrics().Len())

	for i := 0; i < sm.Metrics().Len(); i++ {
		dp := sm.Metrics().At(i).Gauge().DataPoints().At(0)
		attrs := dp.Attributes()

		nodeName, ok := attrs.Get("node_name")
		require.True(t, ok)
		assert.Equal(t, "my-node", nodeName.Str())

		instanceID, ok := attrs.Get("instance_id")
		require.True(t, ok)
		assert.Equal(t, "my-node", instanceID.Str())

		clusterName, ok := attrs.Get("cluster_name")
		require.True(t, ok)
		assert.Equal(t, "test-cluster", clusterName.Str())
	}
}

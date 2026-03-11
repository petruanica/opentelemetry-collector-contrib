// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awsekshyperpodreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsekshyperpodreceiver"

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/k8s/k8sclient"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/aws/k8s/k8sutil"
)

const hyperPodPrefix = "hyperpod-"

var allStatuses = []k8sutil.HyperPodConditionType{
	k8sutil.Schedulable,
	k8sutil.UnschedulablePendingReplacement,
	k8sutil.UnschedulablePendingReboot,
	k8sutil.Unschedulable,
}

type scraper struct {
	config     *Config
	logger     *zap.Logger
	k8sClient  *k8sclient.K8sClient
	nodeClient k8sclient.NodeClient
}

func newScraper(config *Config, settings receiver.Settings) *scraper {
	return &scraper{
		config: config,
		logger: settings.Logger,
	}
}

func (s *scraper) start(_ context.Context, _ component.Host) error {
	s.logger.Info("Starting HyperPod health receiver",
		zap.String("cluster", s.config.ClusterName),
		zap.Duration("interval", s.config.CollectionInterval),
	)

	client := k8sclient.Get(s.logger, k8sclient.CaptureOnlyNodeLabelsInfo(true))
	if client == nil {
		return errors.New("failed to initialize K8s client")
	}
	s.k8sClient = client
	s.nodeClient = client.GetNodeClient()

	return nil
}

func (s *scraper) shutdown(_ context.Context) error {
	s.logger.Info("Shutting down HyperPod health receiver")
	if s.k8sClient != nil {
		s.k8sClient.Shutdown()
	}
	return nil
}

func (s *scraper) scrape(_ context.Context) (pmetric.Metrics, error) {
	nodeInfos := s.nodeClient.NodeInfos()
	nodeToLabelsMap := s.nodeClient.NodeToLabelsMap()

	s.logger.Debug("Collected nodes",
		zap.Int("nodeInfos", len(nodeInfos)),
		zap.Int("nodesWithLabels", len(nodeToLabelsMap)),
	)

	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	now := pcommon.NewTimestampFromTime(time.Now())
	for nodeName, labelsMap := range nodeToLabelsMap {
		nodeInfo := nodeInfos[nodeName]
		s.processNode(nodeName, nodeInfo, labelsMap, sm, now)
	}

	return metrics, nil
}

func (s *scraper) processNode(nodeName string, _ *k8sclient.NodeInfo, labelsMap map[k8sclient.Label]int8, sm pmetric.ScopeMetrics, timestamp pcommon.Timestamp) {
	// Get health status from labels map.
	healthStatusInt, ok := labelsMap[k8sclient.SageMakerNodeHealthStatus]
	if !ok {
		s.logger.Debug("Node missing health status label",
			zap.String("node", nodeName),
		)
		return
	}

	// Validate health status value.
	if !isValidHealthStatus(healthStatusInt) {
		s.logger.Warn("Invalid health status value",
			zap.String("node", nodeName),
			zap.Int8("status", healthStatusInt),
		)
		return
	}

	// Convert int8 to status string.
	healthStatus := k8sutil.HyperPodConditionType(healthStatusInt).String()

	// Extract instance ID (remove hyperpod- prefix if present).
	instanceID := strings.TrimPrefix(nodeName, hyperPodPrefix)

	// Emit metrics for all statuses (1 for current, 0 for others).
	s.emitHealthMetrics(sm, nodeName, instanceID, healthStatus, timestamp)
}

func (s *scraper) emitHealthMetrics(sm pmetric.ScopeMetrics, nodeName, instanceID, currentStatus string, timestamp pcommon.Timestamp) {
	for _, status := range allStatuses {
		statusStr := status.String()
		value := int64(0)
		if statusStr == currentStatus {
			value = 1
		}

		metricName := statusToMetricName[statusStr]
		metric := sm.Metrics().AppendEmpty()
		metric.SetName(metricName)
		metric.SetDescription(fmt.Sprintf("HyperPod node health status: %s", statusStr))
		metric.SetUnit("1")

		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetTimestamp(timestamp)
		dp.SetIntValue(value)

		// Add attributes.
		attrs := dp.Attributes()
		attrs.PutStr("node_name", nodeName)
		attrs.PutStr("instance_id", instanceID)
		if s.config.ClusterName != "" {
			attrs.PutStr("cluster_name", s.config.ClusterName)
		}
	}
}

// statusToMetricName maps each HyperPodConditionType string to its full metric name.
var statusToMetricName = map[string]string{
	k8sutil.Schedulable.String():                     "hyperpod_node_health_status_schedulable",
	k8sutil.UnschedulablePendingReplacement.String(): "hyperpod_node_health_status_unschedulable_pending_replacement",
	k8sutil.UnschedulablePendingReboot.String():      "hyperpod_node_health_status_unschedulable_pending_reboot",
	k8sutil.Unschedulable.String():                   "hyperpod_node_health_status_unschedulable",
}

func isValidHealthStatus(status int8) bool {
	return status >= int8(k8sutil.Schedulable) && status <= int8(k8sutil.Unschedulable)
}

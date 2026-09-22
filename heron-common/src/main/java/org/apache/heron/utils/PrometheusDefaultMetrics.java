package org.apache.heron.utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.heron.api.metric.IMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;

public class PrometheusDefaultMetrics implements IMetric<Map<String, String>>, Serializable {
    protected static final Logger logger = LoggerFactory.getLogger(PrometheusDefaultMetrics.class);
    private static final long serialVersionUID = 2639778192964949186L;

    private final transient PrometheusRegistry registry;

    public PrometheusDefaultMetrics() {
        this(PrometheusRegistry.defaultRegistry);
    }

    public PrometheusDefaultMetrics(PrometheusRegistry registry) {
        this.registry = registry != null ? registry : PrometheusRegistry.defaultRegistry;
    }

    private PrometheusRegistry getRegistry() {
        return registry != null ? registry : PrometheusRegistry.defaultRegistry;
    }

    @Override
    public Map<String, String> getValueAndReset() {
        final var metrics = new HashMap<String, String>();
        try {
            getRegistry().scrape().forEach(snapshot -> {
                if (snapshot.getDataPoints().isEmpty()) {
                    return;
                }
                var metaData = snapshot.getMetadata();
                if (snapshot instanceof CounterSnapshot) {
                    var counterSnapshot = (CounterSnapshot) snapshot;
                    for (var data : counterSnapshot.getDataPoints()) {
                        metrics.put(
                                getMetricNameWithLabels(metaData.getPrometheusName(), data.getLabels()),
                                String.valueOf(data.getValue()));
                    }
                } else if (snapshot instanceof GaugeSnapshot) {
                    var gaugeSnapshot = (GaugeSnapshot) snapshot;
                    for (var data : gaugeSnapshot.getDataPoints()) {
                        metrics.put(
                                getMetricNameWithLabels(metaData.getPrometheusName(), data.getLabels()),
                                String.valueOf(data.getValue()));
                    }
                }
            });
        } catch (Exception e) {
            logger.warn("Failed to scrape Prometheus metrics: {}", e.getMessage(), e);
        }
        logger.debug("metrics: {}", metrics);
        return metrics;
    }

    public static String getMetricNameWithLabels(String name, Labels labels) {
        if (labels == null || labels.isEmpty()) {
            return name;
        }
        final var namedLabels = new StringBuilder(name.length() + labels.size() * 32);
        namedLabels.append(name).append("{");
        for (int i = 0; i < labels.size(); i++) {
            if (i > 0) {
                namedLabels.append(",");
            }
            namedLabels.append(labels.getPrometheusName(i))
                       .append("=\"")
                       .append(escapeLabelValue(labels.getValue(i)))
                       .append("\"");
        }
        namedLabels.append("}");
        return namedLabels.toString();
    }

    private static String escapeLabelValue(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n");
    }
}

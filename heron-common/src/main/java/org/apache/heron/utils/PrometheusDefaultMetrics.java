package org.apache.heron.utils;

import java.io.Serial;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.heron.api.metric.IMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;

public class PrometheusDefaultMetrics implements IMetric<Map<String, String>>, Serializable {
    protected static final Logger logger = LoggerFactory.getLogger(PrometheusDefaultMetrics.class);
    @Serial
    private static final long serialVersionUID = 2639778192964949186L;

    public PrometheusDefaultMetrics() {
    }

    @Override
    public Map<String, String> getValueAndReset() {
        final var metrics = new HashMap<String, String>();
        try {
            PrometheusRegistry.defaultRegistry.scrape().forEach(snapshot -> {
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
                    logger.info("CounterSnapshot: {}", metaData.getPrometheusName());
                } else if (snapshot instanceof GaugeSnapshot) {
                    var gaugeSnapshot = (GaugeSnapshot) snapshot;
                    for (var data : gaugeSnapshot.getDataPoints()) {
                        metrics.put(
                                getMetricNameWithLabels(metaData.getPrometheusName(), data.getLabels()),
                                String.valueOf(data.getValue()));
                    }
                    logger.info("GaugeSnapshot: {}", metaData.getPrometheusName());
                } else if (snapshot instanceof HistogramSnapshot) {
                    // HistogramSnapshot is not supported yet
                    logger.info("HistogramSnapshot: {} not supported.", metaData.getPrometheusName());
                } else {
                    logger.info("Snapshot: {} not supported.", snapshot.getClass().getName());
                }
            });
        } catch (Throwable t) {
            logger.info(t.getMessage(), t);
        }
        logger.debug("metrics: {}", metrics);
        return metrics;
    }

    public static String getMetricNameWithLabels(String name, Labels labels) {
        final var namedLabels = new StringBuilder(name);
        if (labels.isEmpty()) {
            return namedLabels.toString();
        }
        namedLabels.append("{");
        for (int i = 0; i < labels.size(); i++) {
            if (i > 0) {
                namedLabels.append(",");
            }
            // Escape labels or avoid duplicates if needed
            namedLabels.append(labels.getPrometheusName(i))
                       .append("=\"")
                       .append(labels.getValue(i))
                       .append("\"");
        }
        namedLabels.append("}");
        return namedLabels.toString();
    }

}

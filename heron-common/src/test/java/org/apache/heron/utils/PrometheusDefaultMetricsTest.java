package org.apache.heron.utils;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.Labels;

public class PrometheusDefaultMetricsTest {

    private PrometheusRegistry customRegistry;

    @BeforeMethod
    public void setUp() {
        customRegistry = new PrometheusRegistry();
    }

    @Test
    public void testEmptyRegistry() {
        PrometheusDefaultMetrics metrics = new PrometheusDefaultMetrics(customRegistry);
        Map<String, String> result = metrics.getValueAndReset();
        Assert.assertNotNull(result);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testCounterAndGaugeMetrics() {
        Counter counter = Counter.builder()
                .name("test_counter")
                .help("test counter help")
                .labelNames("env", "tier")
                .register(customRegistry);
        counter.labelValues("prod", "backend").inc(5.0);

        Gauge gauge = Gauge.builder()
                .name("test_gauge")
                .help("test gauge help")
                .register(customRegistry);
        gauge.set(99.5);

        PrometheusDefaultMetrics metrics = new PrometheusDefaultMetrics(customRegistry);
        Map<String, String> result = metrics.getValueAndReset();

        Assert.assertEquals(result.get("test_counter{env=\"prod\",tier=\"backend\"}"), "5.0");
        Assert.assertEquals(result.get("test_gauge"), "99.5");
    }

    @Test
    public void testGetMetricNameWithLabels() {
        Labels labels = Labels.of(new String[]{"app", "region"}, new String[]{"heron", "us-east-1"});
        String name = PrometheusDefaultMetrics.getMetricNameWithLabels("jvm_memory", labels);
        Assert.assertEquals(name, "jvm_memory{app=\"heron\",region=\"us-east-1\"}");

        String emptyLabelName = PrometheusDefaultMetrics.getMetricNameWithLabels("jvm_memory", Labels.EMPTY);
        Assert.assertEquals(emptyLabelName, "jvm_memory");

        String nullLabelName = PrometheusDefaultMetrics.getMetricNameWithLabels("jvm_memory", null);
        Assert.assertEquals(nullLabelName, "jvm_memory");
    }

    @Test
    public void testLabelValueEscaping() {
        Labels labels = Labels.of(new String[]{"path", "quote"}, new String[]{"/api/v1\nline", "val\"ue\\test"});
        String name = PrometheusDefaultMetrics.getMetricNameWithLabels("http_requests", labels);
        Assert.assertEquals(name, "http_requests{path=\"/api/v1\\nline\",quote=\"val\\\"ue\\\\test\"}");
    }
}

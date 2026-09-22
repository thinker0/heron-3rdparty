/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.heron.kafka.spout;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.heron.kafka.spout.internal.OffsetManager;
import org.apache.heron.kafka.spout.metrics.KafkaOffsetMetric;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KafkaOffsetMetricTest {

    @Test
    public void testComputeMetricsCalculation() {
        TopicPartition tp0 = new TopicPartition("orders", 0);
        TopicPartition tp1 = new TopicPartition("orders", 1);

        OffsetManager om0 = new OffsetManager(tp0, 100L);
        om0.addToEmitMsgs(150L);

        OffsetManager om1 = new OffsetManager(tp1, 200L);
        om1.addToEmitMsgs(230L);

        Map<TopicPartition, OffsetManager> offsetManagers = new HashMap<>();
        offsetManagers.put(tp0, om0);
        offsetManagers.put(tp1, om1);

        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(tp0, 180L);
        endOffsets.put(tp1, 260L);

        Map<String, Long> metrics = KafkaOffsetMetric.computeMetrics(offsetManagers, endOffsets);

        Assert.assertNotNull(metrics);
        // Partition 0
        Assert.assertEquals(metrics.get("orders/partition_0/latestTimeOffset"), Long.valueOf(180L));
        Assert.assertEquals(metrics.get("orders/partition_0/latestEmittedOffset"), Long.valueOf(150L));
        Assert.assertEquals(metrics.get("orders/partition_0/latestCompletedOffset"), Long.valueOf(100L));
        Assert.assertEquals(metrics.get("orders/partition_0/spoutLag"), Long.valueOf(80L)); // 180 - 100

        // Partition 1
        Assert.assertEquals(metrics.get("orders/partition_1/latestTimeOffset"), Long.valueOf(260L));
        Assert.assertEquals(metrics.get("orders/partition_1/latestEmittedOffset"), Long.valueOf(230L));
        Assert.assertEquals(metrics.get("orders/partition_1/latestCompletedOffset"), Long.valueOf(200L));
        Assert.assertEquals(metrics.get("orders/partition_1/spoutLag"), Long.valueOf(60L)); // 260 - 200

        // Topic totals
        Assert.assertEquals(metrics.get("orders/totalLatestTimeOffset"), Long.valueOf(440L));
        Assert.assertEquals(metrics.get("orders/totalLatestEmittedOffset"), Long.valueOf(380L));
        Assert.assertEquals(metrics.get("orders/totalLatestCompletedOffset"), Long.valueOf(300L));
        Assert.assertEquals(metrics.get("orders/totalSpoutLag"), Long.valueOf(140L)); // 80 + 60
    }

    @Test
    public void testRefreshAndGetValueAndReset() {
        TopicPartition tp = new TopicPartition("clicks", 0);
        OffsetManager om = new OffsetManager(tp, 50L);
        om.addToEmitMsgs(75L);

        Map<TopicPartition, OffsetManager> offsetManagers = Collections.singletonMap(tp, om);

        @SuppressWarnings("unchecked")
        Consumer<Object, Object> mockConsumer = Mockito.mock(Consumer.class);
        Mockito.when(mockConsumer.endOffsets(Mockito.anyCollection()))
            .thenReturn(Collections.singletonMap(tp, 100L));

        KafkaOffsetMetric<Object, Object> metric = new KafkaOffsetMetric<>(
            () -> offsetManagers,
            () -> mockConsumer
        );

        // Explicit refresh on spout thread
        metric.refresh();

        // Metric thread reads snapshot
        Object value = metric.getValueAndReset();
        Assert.assertTrue(value instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Long> resultMap = (Map<String, Long>) value;

        Assert.assertEquals(resultMap.get("clicks/partition_0/spoutLag"), Long.valueOf(50L)); // 100 - 50
        Assert.assertEquals(resultMap.get("clicks/totalSpoutLag"), Long.valueOf(50L));
    }

    @Test
    public void testComputeMetricsNullSafety() {
        Assert.assertTrue(KafkaOffsetMetric.computeMetrics(null, Collections.emptyMap()).isEmpty());
        Assert.assertTrue(KafkaOffsetMetric.computeMetrics(Collections.emptyMap(), null).isEmpty());
    }
}

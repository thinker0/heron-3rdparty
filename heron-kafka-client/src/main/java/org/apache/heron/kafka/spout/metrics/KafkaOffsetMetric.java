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

package org.apache.heron.kafka.spout.metrics;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.heron.api.metric.IMetric;
import org.apache.heron.kafka.spout.internal.OffsetManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used compute the partition and topic level offset metrics.
 * <p>
 * Partition level metrics are:
 * topicName/partition_{number}/latestTimeOffset //gives end offset of the partition
 * topicName/partition_{number}/latestEmittedOffset //gives latest emitted offset of the partition from the spout
 * topicName/partition_{number}/latestCompletedOffset //gives latest committed offset of the partition from the spout
 * topicName/partition_{number}/spoutLag // the delta between the latest Offset and latestCompletedOffset
 * </p>
 * <p>
 * Topic level metrics are:
 * topicName/totalLatestTimeOffset //gives the total end offset of all the associated partitions of this spout
 * topicName/totalLatestEmittedOffset //gives the total latest emitted offset of all the associated partitions of this spout
 * topicName/totalLatestCompletedOffset //gives the total latest committed offset of all the associated partitions of this spout
 * topicName/totalSpoutLag // total spout lag of all the associated partitions of this spout
 * </p>
 */
public class KafkaOffsetMetric<K, V> implements IMetric<Map<String, Long>> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetMetric.class);
    private final Supplier<Map<TopicPartition, OffsetManager>> offsetManagerSupplier;
    private final Supplier<Consumer<K, V>> consumerSupplier;
    private final AtomicReference<Map<String, Long>> latestMetrics = new AtomicReference<>(Collections.emptyMap());

    public KafkaOffsetMetric(Supplier<Map<TopicPartition, OffsetManager>> offsetManagerSupplier,
        Supplier<Consumer<K, V>> consumerSupplier) {
        this.offsetManagerSupplier = offsetManagerSupplier;
        this.consumerSupplier = consumerSupplier;
    }

    /**
     * Refreshes metrics on the Spout main thread where Consumer is safely accessible.
     */
    public void refresh() {
        Map<TopicPartition, OffsetManager> offsetManagers = offsetManagerSupplier != null ? offsetManagerSupplier.get() : null;
        Consumer<K, V> consumer = consumerSupplier != null ? consumerSupplier.get() : null;

        if (offsetManagers == null || offsetManagers.isEmpty() || consumer == null) {
            LOG.debug("Metrics refresh skipped: offsetManagers or consumer is null/empty.");
            return;
        }

        try {
            Set<TopicPartition> topicPartitions = offsetManagers.keySet();
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
            Map<String, Long> result = computeMetrics(offsetManagers, endOffsets);
            latestMetrics.set(Collections.unmodifiableMap(result));
        } catch (RetriableException e) {
            LOG.warn("Failed to get offsets from Kafka! Will retry on next metrics refresh.", e);
        } catch (Exception e) {
            LOG.warn("Unexpected error refreshing Kafka offset metrics.", e);
        }
    }

    public static Map<String, Long> computeMetrics(Map<TopicPartition, OffsetManager> offsetManagers,
                                                   Map<TopicPartition, Long> endOffsets) {
        if (offsetManagers == null || offsetManagers.isEmpty() || endOffsets == null) {
            return Collections.emptyMap();
        }

        Map<String, TopicMetrics> topicMetricsMap = new HashMap<>();
        Map<String, Long> result = new HashMap<>();

        for (Map.Entry<TopicPartition, OffsetManager> entry : offsetManagers.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            OffsetManager offsetManager = entry.getValue();

            Long latestTimeOffset = endOffsets.get(topicPartition);
            if (latestTimeOffset == null || offsetManager == null) {
                continue;
            }

            long latestEmittedOffset = offsetManager.getLatestEmittedOffset();
            long latestCompletedOffset = offsetManager.getCommittedOffset();
            long spoutLag = latestTimeOffset - latestCompletedOffset;

            String metricPath = topicPartition.topic() + "/partition_" + topicPartition.partition();
            result.put(metricPath + "/spoutLag", spoutLag);
            result.put(metricPath + "/latestTimeOffset", latestTimeOffset);
            result.put(metricPath + "/latestEmittedOffset", latestEmittedOffset);
            result.put(metricPath + "/latestCompletedOffset", latestCompletedOffset);

            TopicMetrics topicMetrics = topicMetricsMap.computeIfAbsent(topicPartition.topic(), k -> new TopicMetrics());
            topicMetrics.totalSpoutLag += spoutLag;
            topicMetrics.totalLatestTimeOffset += latestTimeOffset;
            topicMetrics.totalLatestEmittedOffset += latestEmittedOffset;
            topicMetrics.totalLatestCompletedOffset += latestCompletedOffset;
        }

        for (Map.Entry<String, TopicMetrics> e : topicMetricsMap.entrySet()) {
            String topic = e.getKey();
            TopicMetrics topicMetrics = e.getValue();
            result.put(topic + "/totalSpoutLag", topicMetrics.totalSpoutLag);
            result.put(topic + "/totalLatestTimeOffset", topicMetrics.totalLatestTimeOffset);
            result.put(topic + "/totalLatestEmittedOffset", topicMetrics.totalLatestEmittedOffset);
            result.put(topic + "/totalLatestCompletedOffset", topicMetrics.totalLatestCompletedOffset);
        }

        return result;
    }

    @Override
    public Map<String, Long> getValueAndReset() {
        Map<String, Long> metrics = latestMetrics.get();
        if (metrics == null || metrics.isEmpty()) {
            LOG.debug("Metrics Tick: no metrics available.");
            return null;
        }

        LOG.debug("Metrics Tick: value : {}", metrics);
        return new HashMap<>(metrics);
    }

    private static class TopicMetrics {
        long totalSpoutLag = 0;
        long totalLatestTimeOffset = 0;
        long totalLatestEmittedOffset = 0;
        long totalLatestCompletedOffset = 0;
    }
}

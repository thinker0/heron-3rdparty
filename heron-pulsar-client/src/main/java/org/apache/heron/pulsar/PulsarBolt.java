/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.heron.pulsar;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.heron.api.bolt.BaseRichBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.metric.IMetric;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.utils.TupleUtils;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarBolt extends BaseRichBolt implements IMetric<Map<String, Object>> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PulsarBolt.class);

    public static final String NO_OF_MESSAGES_SENT = "numberOfMessagesSent";
    public static final String NO_OF_MESSAGES_FAILED = "numberOfMessagesFailed";
    public static final String PRODUCER_RATE = "producerRate";
    public static final String PRODUCER_THROUGHPUT_BYTES = "producerThroughput";

    private final ClientConfigurationData clientConf;
    private final ProducerConfigurationData producerConf;
    private final PulsarBoltConfiguration pulsarBoltConf;
    private final ConcurrentMap<String, Object> metricsMap = new ConcurrentHashMap<>();

    private SharedPulsarClient sharedPulsarClient;
    private String componentId;
    private String boltId;
    private OutputCollector collector;
    private Producer<byte[]> producer;
    private final AtomicLong messagesSent = new AtomicLong();
    private final AtomicLong messagesFailed = new AtomicLong();
    private final AtomicLong messageSizeSent = new AtomicLong();

    public PulsarBolt(PulsarBoltConfiguration pulsarBoltConf) {
        this(pulsarBoltConf, PulsarClient.builder());
    }

    public PulsarBolt(PulsarBoltConfiguration pulsarBoltConf, ClientBuilder clientBuilder) {
        this(pulsarBoltConf,
                ((ClientBuilderImpl) requireNonNull(clientBuilder, "clientBuilder cannot be null"))
                        .getClientConfigurationData().clone(),
                new ProducerConfigurationData());
    }

    public PulsarBolt(PulsarBoltConfiguration pulsarBoltConf, ClientConfigurationData clientConf,
            ProducerConfigurationData producerConf) {
        requireNonNull(pulsarBoltConf, "bolt configuration can't be null");
        requireNonNull(clientConf, "client configuration can't be null");
        requireNonNull(producerConf, "producer configuration can't be null");
        requireNonNull(pulsarBoltConf.getServiceUrl(), "serviceUrl can't be null");
        requireNonNull(pulsarBoltConf.getTopic(), "topic can't be null in PulsarBoltConfiguration");
        requireNonNull(pulsarBoltConf.getTupleToMessageMapper(), "tuple mapper can't be null");
        this.pulsarBoltConf = pulsarBoltConf;
        this.clientConf = clientConf;
        this.producerConf = producerConf;
        this.clientConf.setServiceUrl(pulsarBoltConf.getServiceUrl());
        this.producerConf.setTopicName(pulsarBoltConf.getTopic());
        this.producerConf.setBatcherBuilder(null);
    }

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
        this.componentId = context.getThisComponentId();
        this.boltId = String.format("%s-%s", componentId, context.getThisTaskId());
        this.collector = collector;
        try {
            sharedPulsarClient = SharedPulsarClient.get(componentId, clientConf);
            producer = sharedPulsarClient.getSharedProducer(producerConf);
            LOG.info("[{}] Created a pulsar producer on topic {} to send messages", boltId, pulsarBoltConf.getTopic());
        } catch (PulsarClientException e) {
            LOG.error("[{}] Error initializing pulsar producer on topic {}", boltId, pulsarBoltConf.getTopic(), e);
            throw new IllegalStateException(
                    format("Failed to initialize producer for %s : %s", pulsarBoltConf.getTopic(), e.getMessage()), e);
        }
        context.registerMetric(String.format("PulsarBolt/%s-%s", componentId, context.getThisTaskIndex()), this,
                pulsarBoltConf.getMetricsTimeIntervalInSecs());
    }

    @Override
    public void execute(Tuple input) {
        if (TupleUtils.isTick(input)) {
            synchronized (collector) {
                collector.ack(input);
            }
            return;
        }
        if (producer == null) {
            IllegalStateException ex = new IllegalStateException("Pulsar producer is not initialized");
            LOG.error("[{}] Producer is null, failing tuple", boltId, ex);
            synchronized (collector) {
                collector.reportError(ex);
                collector.fail(input);
            }
            messagesFailed.incrementAndGet();
            return;
        }
        try {
            TypedMessageBuilder<byte[]> msgBuilder = pulsarBoltConf.getTupleToMessageMapper()
                    .toMessage(producer.newMessage(), input);
            if (msgBuilder == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[{}] Cannot send null message, acking the collector", boltId);
                }
                synchronized (collector) {
                    collector.ack(input);
                }
            } else {
                long size = 0;
                if (msgBuilder instanceof TypedMessageBuilderImpl) {
                    ByteBuffer content = ((TypedMessageBuilderImpl<byte[]>) msgBuilder).getContent();
                    if (content != null) {
                        size = content.remaining();
                    }
                }
                final long messageSizeToBeSent = size;
                msgBuilder.sendAsync().handle((msgId, ex) -> {
                    synchronized (collector) {
                        if (ex != null) {
                            collector.reportError(ex);
                            collector.fail(input);
                            messagesFailed.incrementAndGet();
                            LOG.error("[{}] Message send failed", boltId, ex);
                        } else {
                            collector.ack(input);
                            messagesSent.incrementAndGet();
                            messageSizeSent.addAndGet(messageSizeToBeSent);
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("[{}] Message sent with id {}", boltId, msgId);
                            }
                        }
                    }
                    return null;
                });
            }
        } catch (Exception e) {
            LOG.error("[{}] Message processing failed", boltId, e);
            synchronized (collector) {
                collector.reportError(e);
                collector.fail(input);
            }
            messagesFailed.incrementAndGet();
        }
    }

    public void close() {
        try {
            LOG.info("[{}] Closing Pulsar producer on topic {}", boltId, pulsarBoltConf.getTopic());
            if (producer != null) {
                try {
                    producer.flush();
                } catch (PulsarClientException e) {
                    LOG.warn("[{}] Error flushing Pulsar producer on topic {}", boltId, pulsarBoltConf.getTopic(), e);
                }
            }
            if (sharedPulsarClient != null) {
                sharedPulsarClient.close();
            }
        } catch (PulsarClientException e) {
            LOG.error("[{}] Error closing Pulsar producer on topic {}", boltId, pulsarBoltConf.getTopic(), e);
        }
    }

    @Override
    public void cleanup() {
        close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        pulsarBoltConf.getTupleToMessageMapper().declareOutputFields(declarer);
    }

    /**
     * Helpers for metrics
     */

    Map<String, Object> getMetrics() {
        long sent = messagesSent.get();
        long failed = messagesFailed.get();
        long bytesSent = messageSizeSent.get();
        metricsMap.put(NO_OF_MESSAGES_SENT, sent);
        metricsMap.put(NO_OF_MESSAGES_FAILED, failed);
        metricsMap.put(PRODUCER_RATE, ((double) sent) / pulsarBoltConf.getMetricsTimeIntervalInSecs());
        metricsMap.put(PRODUCER_THROUGHPUT_BYTES,
                ((double) bytesSent) / pulsarBoltConf.getMetricsTimeIntervalInSecs());
        return metricsMap;
    }

    void resetMetrics() {
        messagesSent.set(0);
        messagesFailed.set(0);
        messageSizeSent.set(0);
    }

    @Override
    public Map<String, Object> getValueAndReset() {
        Map<String, Object> metrics = new HashMap<>();
        long sent = messagesSent.getAndSet(0);
        long failed = messagesFailed.getAndSet(0);
        long bytesSent = messageSizeSent.getAndSet(0);
        metrics.put(NO_OF_MESSAGES_SENT, sent);
        metrics.put(NO_OF_MESSAGES_FAILED, failed);
        metrics.put(PRODUCER_RATE, ((double) sent) / pulsarBoltConf.getMetricsTimeIntervalInSecs());
        metrics.put(PRODUCER_THROUGHPUT_BYTES,
                ((double) bytesSent) / pulsarBoltConf.getMetricsTimeIntervalInSecs());
        return metrics;
    }
}

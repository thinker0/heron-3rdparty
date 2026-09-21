/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.heron.api.metric.IMetric;
import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.api.utils.Utils;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarSpout extends BaseRichSpout implements IMetric<Map<String, Object>> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PulsarSpout.class);

    public static final String NO_OF_PENDING_FAILED_MESSAGES = "numberOfPendingFailedMessages";
    public static final String NO_OF_MESSAGES_RECEIVED = "numberOfMessagesReceived";
    public static final String NO_OF_MESSAGES_EMITTED = "numberOfMessagesEmitted";
    public static final String NO_OF_MESSAGES_FAILED = "numberOfMessagesFailed";
    public static final String MESSAGE_NOT_AVAILABLE_COUNT = "messageNotAvailableCount";
    public static final String NO_OF_PENDING_ACKS = "numberOfPendingAcks";
    public static final String NO_OF_FAILED_ACKS = "numberOfFailedAcks";
    public static final String CONSUMER_RATE = "consumerRate";
    public static final String CONSUMER_THROUGHPUT_BYTES = "consumerThroughput";

    protected final ClientConfigurationData clientConf;
    protected final PulsarSpoutConfiguration pulsarSpoutConf;
    protected final ConsumerConfigurationData<byte[]> consumerConf;
    private final long failedRetriesTimeoutNano;
    private final int maxFailedRetries;
    private final ConcurrentMap<MessageId, MessageRetries> pendingMessageRetries = new ConcurrentHashMap<>();
    private final Queue<Message<byte[]>> failedMessages = new ConcurrentLinkedQueue<>();
    private final ConcurrentMap<String, Object> metricsMap = new ConcurrentHashMap<>();

    private SharedPulsarClient sharedPulsarClient;
    private String componentId;
    private String spoutId;
    private SpoutOutputCollector collector;
    private PulsarSpoutConsumer consumer;
    private final AtomicLong messagesReceived = new AtomicLong();
    private final AtomicLong messagesEmitted = new AtomicLong();
    private final AtomicLong messagesFailed = new AtomicLong();
    private final AtomicLong messageNotAvailableCount = new AtomicLong();
    private final AtomicLong pendingAcks = new AtomicLong();
    private final AtomicLong failedAcks = new AtomicLong();
    private final AtomicLong messageSizeReceived = new AtomicLong();

    public PulsarSpout(PulsarSpoutConfiguration pulsarSpoutConf) {
        this(pulsarSpoutConf, PulsarClient.builder());
    }

    public PulsarSpout(PulsarSpoutConfiguration pulsarSpoutConf, ClientBuilder clientBuilder) {
        this(pulsarSpoutConf,
                ((ClientBuilderImpl) requireNonNull(clientBuilder, "clientBuilder cannot be null"))
                        .getClientConfigurationData().clone(),
                new ConsumerConfigurationData<byte[]>());
    }

    public PulsarSpout(PulsarSpoutConfiguration pulsarSpoutConf, ClientConfigurationData clientConfig,
                       ConsumerConfigurationData<byte[]> consumerConfig) {
        requireNonNull(pulsarSpoutConf, "spout configuration can't be null");
        requireNonNull(pulsarSpoutConf.getServiceUrl(), "serviceUrl can't be null");
        requireNonNull(pulsarSpoutConf.getSubscriptionName(), "subscriptionName can't be null");
        requireNonNull(pulsarSpoutConf.getMessageToValuesMapper(), "messageToValuesMapper can't be null");
        if (Objects.isNull(pulsarSpoutConf.getTopicNames())
            && Objects.isNull(pulsarSpoutConf.getTopic())
            && Objects.isNull((pulsarSpoutConf.getTopicPattern()))) {
            throw new IllegalArgumentException("names or pattern of Topic");
        }

        requireNonNull(clientConfig, "client configuration can't be null");
        requireNonNull(consumerConfig, "consumer configuration can't be null");
        this.clientConf = clientConfig;
        this.clientConf.setServiceUrl(pulsarSpoutConf.getServiceUrl());
        this.consumerConf = consumerConfig;
        this.pulsarSpoutConf = pulsarSpoutConf;
        this.failedRetriesTimeoutNano = pulsarSpoutConf.getFailedRetriesTimeout(TimeUnit.NANOSECONDS);
        this.maxFailedRetries = pulsarSpoutConf.getMaxFailedRetries();
    }

    @Override
    public void close() {
        try {
            LOG.info("[{}] Closing Pulsar consumer for topic {}", spoutId,
                     pulsarSpoutConf.getTopicNameOrPattern());

            if (pulsarSpoutConf.isAutoUnsubscribe() && !pulsarSpoutConf.isSharedConsumerEnabled() && consumer != null) {
                try {
                    consumer.unsubscribe();
                } catch (PulsarClientException e) {
                    LOG.error("[{}] Failed to unsubscribe {} on topic {}", spoutId,
                              pulsarSpoutConf.getSubscriptionName(),
                              pulsarSpoutConf.getTopicNameOrPattern(), e);
                }
            }

            if (!pulsarSpoutConf.isSharedConsumerEnabled() && consumer != null) {
                consumer.close();
            }
            if (sharedPulsarClient != null) {
                sharedPulsarClient.close();
            }
            pendingMessageRetries.clear();
            failedMessages.clear();
        } catch (PulsarClientException e) {
            LOG.error("[{}] Error closing Pulsar consumer for topic {}", spoutId, pulsarSpoutConf.getTopic(), e);
        }
    }

    @Override
    public void ack(Object msgId) {
        if (msgId instanceof Message) {
            Message<?> msg = (Message<?>) msgId;
            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}] Received ack for message {}", spoutId, msg.getMessageId());
            }
            if (consumer != null) {
                CompletableFuture<Void> future = consumer.acknowledgeAsync(msg);
                if (future != null) {
                    future.whenComplete((result, throwable) -> {
                        if (throwable != null) {
                            failedAcks.incrementAndGet();
                            LOG.warn("[{}] Failed to acknowledge message {}", spoutId, msg.getMessageId(), throwable);
                        }
                    });
                }
            }
            pendingMessageRetries.remove(msg.getMessageId());
            pendingAcks.decrementAndGet();
        }
    }

    @Override
    public void fail(Object msgId) {
        if (msgId instanceof Message) {
            @SuppressWarnings("unchecked")
            Message<byte[]> msg = (Message<byte[]>) msgId;
            MessageId id = msg.getMessageId();
            LOG.warn("[{}] Error processing message {}", spoutId, id);

            // Since the message processing failed, we put it in the failed messages queue if there are more retries
            // remaining for the message
            MessageRetries messageRetries = pendingMessageRetries.computeIfAbsent(id, (k) -> new MessageRetries());
            if ((failedRetriesTimeoutNano < 0
                    || (messageRetries.getTimeStamp() + failedRetriesTimeoutNano) > System.nanoTime())
                    && (maxFailedRetries < 0 || messageRetries.getNumRetries() < maxFailedRetries)) {
                // record latest failure timestamp and increment retry count
                LOG.info("[{}] Putting message {} in the retry queue", spoutId, id);
                messageRetries.recordFailure();
                failedMessages.add(msg);
                pendingAcks.decrementAndGet();
                messagesFailed.incrementAndGet();
            } else {
                LOG.warn("[{}] Number of retries limit reached, dropping the message {}", spoutId, id);
                ack(msg);
            }
        }
    }

    /**
     * Emits a tuple received from the Pulsar consumer unless there are any failed messages
     */
    @Override
    public void nextTuple() {
        emitNextAvailableTuple();
    }

    /**
     * It makes sure that it emits next available non-tuple to topology unless consumer queue doesn't have any message
     * available. It receives message from consumer queue and converts it to tuple and emits to topology. if the
     * converted tuple is null then it tries to receives next message and perform the same until it finds non-tuple to
     * emit.
     */
    public void emitNextAvailableTuple() {
        // check if there are any failed messages to re-emit in the topology
        if (emitFailedMessage()) {
            return;
        }

        Message<byte[]> msg;
        // receive from consumer if no failed messages
        if (consumer != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}] Receiving the next message from pulsar consumer to emit to the collector", spoutId);
            }
            try {
                boolean done = false;
                while (!done) {
                    msg = consumer.receive(100, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        messagesReceived.incrementAndGet();
                        messageSizeReceived.getAndAdd(msg.getData() != null ? msg.getData().length : 0);
                        done = mapToValueAndEmit(msg);
                    } else {
                        // queue is empty and nothing to emit
                        done = true;
                        messageNotAvailableCount.incrementAndGet();
                    }
                }
            } catch (PulsarClientException e) {
                LOG.error("[{}] Error receiving message from pulsar consumer", spoutId, e);
                Utils.sleep(100);
            }
        }
    }

    private boolean emitFailedMessage() {
        Message<byte[]> msg;

        while ((msg = failedMessages.peek()) != null) {
            MessageRetries messageRetries = pendingMessageRetries.get(msg.getMessageId());
            if (messageRetries != null) {
                // check if retry needs backoff
                if (Backoff.shouldBackoff(messageRetries.getTimeStamp(), TimeUnit.NANOSECONDS,
                        messageRetries.getNumRetries(), clientConf.getInitialBackoffIntervalNanos(),
                        clientConf.getMaxBackoffIntervalNanos())) {
                    // Non-blocking: Do not sleep main thread; return false to allow processing live incoming messages
                    return false;
                } else {
                    // remove the message from the queue and emit to the topology
                    LOG.info("[{}] Retrying failed message {}", spoutId, msg.getMessageId());
                    failedMessages.remove();
                    mapToValueAndEmit(msg);
                    return true;
                }
            }

            // messageRetries is null because messageRetries is already acked and removed from pendingMessageRetries
            // then remove it from failed message queue as well.
            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}]-{} removing {} from failedMessage because it's already acked",
                          pulsarSpoutConf.getTopicNameOrPattern(), spoutId, msg.getMessageId());
            }
            failedMessages.remove();
        }
        return false;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.componentId = context.getThisComponentId();
        this.spoutId = String.format("%s-%s", componentId, context.getThisTaskId());
        this.collector = collector;
        pendingMessageRetries.clear();
        failedMessages.clear();
        try {
            consumer = createConsumer();
            LOG.info("[{}] Created a pulsar consumer on topic {} to receive messages with subscription {}",
                     spoutId,
                     pulsarSpoutConf.getTopicNameOrPattern(), pulsarSpoutConf.getSubscriptionName());
        } catch (PulsarClientException e) {
            LOG.error("[{}] Error creating pulsar consumer on topic {}", spoutId,
                      pulsarSpoutConf.getTopicNameOrPattern(), e);
            throw new IllegalStateException(format("Failed to initialize consumer for %s-%s : %s",
                                                   pulsarSpoutConf.getTopicNameOrPattern(),
                                                   pulsarSpoutConf.getSubscriptionName(), e.getMessage()), e);
        }
        context.registerMetric(String.format("PulsarSpout/%s-%s", componentId, context.getThisTaskIndex()),
                               this,
                               pulsarSpoutConf.getMetricsTimeIntervalInSecs());
    }

    protected PulsarSpoutConsumer createConsumer() throws PulsarClientException {
        sharedPulsarClient = SharedPulsarClient.get(componentId, clientConf);
        PulsarSpoutConsumer consumer;
        if (pulsarSpoutConf.isSharedConsumerEnabled()) {
            consumer = pulsarSpoutConf.isDurableSubscription()
                       ? new SpoutConsumer(sharedPulsarClient.getSharedConsumer(newConsumerConfiguration()))
                       : new SpoutReader(sharedPulsarClient.getSharedReader(newReaderConfiguration()));
        } else {
            try {
                consumer = pulsarSpoutConf.isDurableSubscription()
                           ? new SpoutConsumer(sharedPulsarClient.getClient()
                                                                 .subscribeAsync(newConsumerConfiguration())
                                                                 .join())
                           : new SpoutReader(sharedPulsarClient.getClient()
                                                               .createReaderAsync(newReaderConfiguration())
                                                               .join());
            } catch (CompletionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof PulsarClientException) {
                    throw (PulsarClientException) cause;
                }
                throw new PulsarClientException(cause != null ? cause : e);
            }
        }
        return consumer;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        pulsarSpoutConf.getMessageToValuesMapper().declareOutputFields(declarer);
    }

    private boolean mapToValueAndEmit(Message<byte[]> msg) {
        if (msg != null) {
            Values values = pulsarSpoutConf.getMessageToValuesMapper().toValues(msg);
            pendingAcks.incrementAndGet();
            if (values == null) {
                // since the mapper returned null, we can drop the message and ack it immediately
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[{}] Dropping message {}", spoutId, msg.getMessageId());
                }
                ack(msg);
            } else {
                if (values instanceof PulsarTuple) {
                    collector.emit(((PulsarTuple) values).getOutputStream(), values, msg);
                } else {
                    collector.emit(values, msg);
                }
                messagesEmitted.incrementAndGet();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[{}] Emitted message {} to the collector", spoutId, msg.getMessageId());
                }
                return true;
            }
        }
        return false;
    }

    public static class MessageRetries {
        private long timestampInNano;
        private int numRetries;

        public MessageRetries() {
            this.timestampInNano = System.nanoTime();
            this.numRetries = 0;
        }

        public long getTimeStamp() {
            return timestampInNano;
        }

        public void recordFailure() {
            this.timestampInNano = System.nanoTime();
            ++this.numRetries;
        }

        public int getNumRetries() {
            return numRetries;
        }
    }

    /**
     * Helpers for metrics
     */

    Map<String, Object> getMetrics() {
        metricsMap.put(NO_OF_PENDING_FAILED_MESSAGES, (long) pendingMessageRetries.size());
        metricsMap.put(NO_OF_MESSAGES_RECEIVED, messagesReceived.get());
        metricsMap.put(NO_OF_MESSAGES_EMITTED, messagesEmitted.get());
        metricsMap.put(NO_OF_MESSAGES_FAILED, messagesFailed.get());
        metricsMap.put(MESSAGE_NOT_AVAILABLE_COUNT, messageNotAvailableCount.get());
        metricsMap.put(NO_OF_PENDING_ACKS, pendingAcks.get());
        metricsMap.put(NO_OF_FAILED_ACKS, failedAcks.get());
        metricsMap.put(CONSUMER_RATE, ((double) messagesReceived.get()) / pulsarSpoutConf.getMetricsTimeIntervalInSecs());
        metricsMap.put(CONSUMER_THROUGHPUT_BYTES,
                ((double) messageSizeReceived.get()) / pulsarSpoutConf.getMetricsTimeIntervalInSecs());
        return metricsMap;
    }

    void resetMetrics() {
        messagesReceived.set(0);
        messagesEmitted.set(0);
        messageSizeReceived.set(0);
        messagesFailed.set(0);
        messageNotAvailableCount.set(0);
        failedAcks.set(0);
    }

    @Override
    public Map<String, Object> getValueAndReset() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put(NO_OF_PENDING_FAILED_MESSAGES, (long) pendingMessageRetries.size());
        metrics.put(NO_OF_PENDING_ACKS, pendingAcks.get());

        long received = messagesReceived.getAndSet(0);
        long emitted = messagesEmitted.getAndSet(0);
        long failed = messagesFailed.getAndSet(0);
        long notAvail = messageNotAvailableCount.getAndSet(0);
        long failedAckCount = failedAcks.getAndSet(0);
        long bytesReceived = messageSizeReceived.getAndSet(0);

        metrics.put(NO_OF_MESSAGES_RECEIVED, received);
        metrics.put(NO_OF_MESSAGES_EMITTED, emitted);
        metrics.put(NO_OF_MESSAGES_FAILED, failed);
        metrics.put(MESSAGE_NOT_AVAILABLE_COUNT, notAvail);
        metrics.put(NO_OF_FAILED_ACKS, failedAckCount);
        metrics.put(CONSUMER_RATE, ((double) received) / pulsarSpoutConf.getMetricsTimeIntervalInSecs());
        metrics.put(CONSUMER_THROUGHPUT_BYTES,
                ((double) bytesReceived) / pulsarSpoutConf.getMetricsTimeIntervalInSecs());
        return metrics;
    }

    protected ReaderConfigurationData<byte[]> newReaderConfiguration() {
        ReaderConfigurationData<byte[]> readerConf = new ReaderConfigurationData<>();
        if (Objects.nonNull(pulsarSpoutConf.getTopicPattern())) {
            throw new IllegalStateException("reader does not support pattern. "
                                            + pulsarSpoutConf.getTopicPattern());
        } else if (Objects.nonNull(pulsarSpoutConf.getTopicNames())) {
            readerConf.setTopicNames(pulsarSpoutConf.getTopicNames());
        } else if (Objects.nonNull(pulsarSpoutConf.getTopic())) {
            readerConf.setTopicNames(Collections.singleton(pulsarSpoutConf.getTopic()));
        }
        readerConf.setTopicName(pulsarSpoutConf.getTopic());
        readerConf.setReaderName(pulsarSpoutConf.getSubscriptionName());
        readerConf.setStartMessageId(pulsarSpoutConf.getNonDurableSubscriptionReadPosition());
        if (this.consumerConf != null) {
            readerConf.setCryptoFailureAction(consumerConf.getCryptoFailureAction());
            readerConf.setCryptoKeyReader(consumerConf.getCryptoKeyReader());
            readerConf.setReadCompacted(consumerConf.isReadCompacted());
            readerConf.setReceiverQueueSize(consumerConf.getReceiverQueueSize());
        }
        return readerConf;
    }

    protected ConsumerConfigurationData<byte[]> newConsumerConfiguration() {
        ConsumerConfigurationData<byte[]> consumerConf = this.consumerConf != null ? this.consumerConf
                                                                                   :
                                                          new ConsumerConfigurationData<>();
        if (Objects.nonNull(pulsarSpoutConf.getTopicNames())) {
            consumerConf.setTopicNames(pulsarSpoutConf.getTopicNames());
            consumerConf.setAutoUpdatePartitions(true);
        } else if (Objects.nonNull(pulsarSpoutConf.getTopicPattern())) {
            consumerConf.setTopicsPattern(pulsarSpoutConf.getTopicPattern());
            consumerConf.setAutoUpdatePartitions(true);
        } else if (Objects.nonNull(pulsarSpoutConf.getTopic())) {
            consumerConf.setTopicNames(Collections.singleton(pulsarSpoutConf.getTopic()));
            consumerConf.setAutoUpdatePartitions(false);
        }
        consumerConf.setSubscriptionName(pulsarSpoutConf.getSubscriptionName());
        consumerConf.setSubscriptionType(pulsarSpoutConf.getSubscriptionType());
        if (this.consumerConf != null) {
            consumerConf.setCryptoFailureAction(consumerConf.getCryptoFailureAction());
            consumerConf.setCryptoKeyReader(consumerConf.getCryptoKeyReader());
            consumerConf.setReadCompacted(consumerConf.isReadCompacted());
            consumerConf.setReceiverQueueSize(consumerConf.getReceiverQueueSize());
        }
        return consumerConf;
    }

    static class SpoutConsumer implements PulsarSpoutConsumer {
        private final Consumer<byte[]> consumer;

        public SpoutConsumer(Consumer<byte[]> consumer) {
            this.consumer = consumer;
        }

        @Override
        public Message<byte[]> receive(int timeout, TimeUnit unit) throws PulsarClientException {
            return consumer.receive(timeout, unit);
        }

        @Override
        public CompletableFuture<Void> acknowledgeAsync(Message<?> msg) {
            return consumer.acknowledgeAsync(msg);
        }

        @Override
        public void close() throws PulsarClientException {
            consumer.close();
        }

        @Override
        public void unsubscribe() throws PulsarClientException {
            consumer.unsubscribe();
        }
    }

    static class SpoutReader implements PulsarSpoutConsumer {
        private final Reader<byte[]> reader;

        public SpoutReader(Reader<byte[]> reader) {
            this.reader = reader;
        }

        @Override
        public Message<byte[]> receive(int timeout, TimeUnit unit) throws PulsarClientException {
            return reader.readNext(timeout, unit);
        }

        @Override
        public CompletableFuture<Void> acknowledgeAsync(Message<?> msg) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() throws PulsarClientException {
            try {
                reader.close();
            } catch (IOException e) {
                throw new PulsarClientException(e);
            }
        }

        @Override
        public void unsubscribe() throws PulsarClientException {
            // No-op
        }
    }
}

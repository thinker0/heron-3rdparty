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

import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SharedPulsarClient {
    private static final Logger LOG = LoggerFactory.getLogger(SharedPulsarClient.class);
    private static final ConcurrentMap<String, SharedPulsarClient> instances = new ConcurrentHashMap<>();

    private final String componentId;
    private final PulsarClientImpl client;
    private final AtomicInteger counter = new AtomicInteger();

    private final ConcurrentMap<String, Consumer<byte[]>> consumers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Reader<byte[]>> readers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Producer<byte[]>> producers = new ConcurrentHashMap<>();

    private SharedPulsarClient(String componentId, ClientConfigurationData clientConf)
            throws PulsarClientException {
        this.client = new PulsarClientImpl(clientConf);
        this.componentId = componentId;
    }

    /**
     * Package-private constructor for dependency injection during testing without spawning Netty thread pools.
     */
    SharedPulsarClient(String componentId, PulsarClientImpl client) {
        this.client = client;
        this.componentId = componentId;
    }

    /**
     * Provides a shared pulsar client that is shared across all different tasks in the same component. Different
     * components will not share the pulsar client since they can have different configurations.
     *
     * @param componentId
     *            the id of the spout/bolt
     * @param clientConf
     *            config
     * @return SharedPulsarClient
     * @throws PulsarClientException
     */
    public static SharedPulsarClient get(String componentId, ClientConfigurationData clientConf)
            throws PulsarClientException {
        Objects.requireNonNull(componentId, "componentId cannot be null");
        Objects.requireNonNull(clientConf, "clientConf cannot be null");
        AtomicReference<PulsarClientException> exception = new AtomicReference<>();
        SharedPulsarClient client = instances.compute(componentId, (k, existing) -> {
            if (existing == null) {
                try {
                    SharedPulsarClient newClient = new SharedPulsarClient(componentId, clientConf);
                    newClient.counter.incrementAndGet();
                    LOG.info("[{}] Created a new Pulsar Client.", componentId);
                    return newClient;
                } catch (PulsarClientException e) {
                    exception.set(e);
                    return null;
                }
            } else {
                if (existing.counter != null) {
                    existing.counter.incrementAndGet();
                }
                return existing;
            }
        });
        if (exception.get() != null) {
            throw exception.get();
        }
        return client;
    }

    public PulsarClientImpl getClient() {
        return client;
    }

    public Consumer<byte[]> getSharedConsumer(ConsumerConfigurationData<byte[]> consumerConf)
            throws PulsarClientException {
        Objects.requireNonNull(consumerConf, "consumerConf cannot be null");
        String key = getConsumerKey(consumerConf);
        synchronized (this) {
            Consumer<byte[]> consumer = consumers.get(key);
            if (consumer == null) {
                try {
                    consumer = client.subscribeAsync(consumerConf).join();
                    consumers.put(key, consumer);
                    LOG.info("[{}] Created a new Pulsar Consumer on {}", componentId, consumerConf.getTopicNames());
                } catch (CompletionException e) {
                    throw unwrapCompletionException(e);
                }
            } else {
                LOG.info("[{}] Using a shared consumer on {}", componentId, consumerConf.getTopicNames());
            }
            return consumer;
        }
    }

    public Reader<byte[]> getSharedReader(ReaderConfigurationData<byte[]> readerConf) throws PulsarClientException {
        Objects.requireNonNull(readerConf, "readerConf cannot be null");
        String key = getReaderKey(readerConf);
        synchronized (this) {
            Reader<byte[]> reader = readers.get(key);
            if (reader == null) {
                try {
                    reader = client.createReaderAsync(readerConf).join();
                    readers.put(key, reader);
                    LOG.info("[{}] Created a new Pulsar reader on {}", componentId, readerConf.getTopicNames());
                } catch (CompletionException e) {
                    throw unwrapCompletionException(e);
                }
            } else {
                LOG.info("[{}] Using a shared reader on {}", componentId, readerConf.getTopicNames());
            }
            return reader;
        }
    }

    public Producer<byte[]> getSharedProducer(ProducerConfigurationData producerConf) throws PulsarClientException {
        Objects.requireNonNull(producerConf, "producerConf cannot be null");
        String key = producerConf.getTopicName();
        Objects.requireNonNull(key, "producer topicName cannot be null");
        synchronized (this) {
            Producer<byte[]> producer = producers.get(key);
            if (producer == null) {
                try {
                    producer = client.createProducerAsync(producerConf).join();
                    producers.put(key, producer);
                    LOG.info("[{}] Created a new Pulsar Producer on {}", componentId, producerConf.getTopicName());
                } catch (CompletionException e) {
                    throw unwrapCompletionException(e);
                }
            } else {
                LOG.info("[{}] Using a shared producer on {}", componentId, producerConf.getTopicName());
            }
            return producer;
        }
    }

    public void close() throws PulsarClientException {
        AtomicReference<PulsarClientException> exceptionRef = new AtomicReference<>();
        instances.compute(componentId, (k, existing) -> {
            if (existing == this) {
                if (counter == null || counter.decrementAndGet() <= 0) {
                    try {
                        closeInternal();
                    } catch (PulsarClientException e) {
                        exceptionRef.set(e);
                    }
                    return null;
                }
            }
            return existing;
        });
        if (exceptionRef.get() != null) {
            throw exceptionRef.get();
        }
    }

    private void closeInternal() throws PulsarClientException {
        synchronized (this) {
            if (producers != null) {
                for (Producer<byte[]> p : producers.values()) {
                    try {
                        p.close();
                    } catch (Exception e) {
                        LOG.warn("[{}] Error closing producer", componentId, e);
                    }
                }
                producers.clear();
            }
            if (consumers != null) {
                for (Consumer<byte[]> c : consumers.values()) {
                    try {
                        c.close();
                    } catch (Exception e) {
                        LOG.warn("[{}] Error closing consumer", componentId, e);
                    }
                }
                consumers.clear();
            }
            if (readers != null) {
                for (Reader<byte[]> r : readers.values()) {
                    try {
                        r.close();
                    } catch (Exception e) {
                        LOG.warn("[{}] Error closing reader", componentId, e);
                    }
                }
                readers.clear();
            }
            if (client != null) {
                try {
                    client.close();
                    LOG.info("[{}] Closed Pulsar Client", componentId);
                } catch (PulsarClientException e) {
                    LOG.error("[{}] Error closing Pulsar Client", componentId, e);
                    throw e;
                }
            }
        }
    }

    public int getReferenceCount() {
        return counter != null ? counter.get() : 0;
    }

    public static void clearInstances() {
        instances.clear();
    }

    public static int getInstanceCount() {
        return instances.size();
    }

    private static String getConsumerKey(ConsumerConfigurationData<byte[]> consumerConf) {
        String topics = consumerConf.getTopicNames() != null
                ? String.join(",", new TreeSet<>(consumerConf.getTopicNames()))
                : "";
        String pattern = consumerConf.getTopicsPattern() != null
                ? consumerConf.getTopicsPattern().pattern()
                : "";
        return String.format("%s:%s:%s:%s",
                consumerConf.getSubscriptionName(),
                consumerConf.getSubscriptionType(),
                topics,
                pattern);
    }

    private static String getReaderKey(ReaderConfigurationData<byte[]> readerConf) {
        String topics = readerConf.getTopicNames() != null
                ? String.join(",", new TreeSet<>(readerConf.getTopicNames()))
                : (readerConf.getTopicName() != null ? readerConf.getTopicName() : "");
        String startMsgId = readerConf.getStartMessageId() != null
                ? readerConf.getStartMessageId().toString()
                : "";
        return String.format("%s:%s:%s",
                readerConf.getReaderName(),
                topics,
                startMsgId);
    }

    private static PulsarClientException unwrapCompletionException(CompletionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof PulsarClientException) {
            return (PulsarClientException) cause;
        }
        return new PulsarClientException(cause != null ? cause : e);
    }
}

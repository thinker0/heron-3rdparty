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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SharedPulsarClientTest {

    private ClientConfigurationData clientConf;

    @BeforeMethod
    public void setUp() {
        SharedPulsarClient.clearInstances();
        clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl("pulsar://localhost:6650");
    }

    @AfterMethod
    public void tearDown() {
        SharedPulsarClient.clearInstances();
    }

    private SharedPulsarClient createMockedSharedClient(String componentId, PulsarClientImpl mockClient) throws Exception {
        String instanceKey = clientConf != null ? SharedPulsarClient.getClientKey(componentId, clientConf) : componentId;
        SharedPulsarClient client = new SharedPulsarClient(componentId, instanceKey, mockClient);

        Field instancesField = SharedPulsarClient.class.getDeclaredField("instances");
        instancesField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<String, SharedPulsarClient> instances =
                (ConcurrentMap<String, SharedPulsarClient>) instancesField.get(null);

        instances.put(instanceKey, client);
        return client;
    }

    @Test
    public void testReferenceCountingAndCloseLifecycle() throws Exception {
        PulsarClientImpl mockClient = mock(PulsarClientImpl.class);
        SharedPulsarClient client1 = createMockedSharedClient("comp-1", mockClient);

        // First reference
        Field counterField = SharedPulsarClient.class.getDeclaredField("counter");
        counterField.setAccessible(true);
        AtomicInteger counter = (AtomicInteger) counterField.get(client1);
        counter.set(1);

        // Acquire second reference via get()
        SharedPulsarClient client2 = SharedPulsarClient.get("comp-1", clientConf);
        assertSame(client1, client2);
        assertEquals(client1.getReferenceCount(), 2);

        // First close should not close client
        client2.close();
        assertEquals(client1.getReferenceCount(), 1);
        assertEquals(SharedPulsarClient.getInstanceCount(), 1);
        verify(mockClient, never()).close();

        // Second close should trigger PulsarClient close and atomically remove instance
        client1.close();
        assertEquals(client1.getReferenceCount(), 0);
        assertEquals(SharedPulsarClient.getInstanceCount(), 0);
        verify(mockClient, times(1)).close();
    }

    @Test
    public void testConcurrentGetAndClose() throws Exception {
        PulsarClientImpl mockClient = mock(PulsarClientImpl.class);
        final String componentId = "comp-concurrent";
        SharedPulsarClient initialClient = createMockedSharedClient(componentId, mockClient);

        Field counterField = SharedPulsarClient.class.getDeclaredField("counter");
        counterField.setAccessible(true);
        AtomicInteger counter = (AtomicInteger) counterField.get(initialClient);
        counter.set(100);

        int threads = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            final boolean isClose = (i % 2 == 0);
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < 50; j++) {
                        if (isClose) {
                            initialClient.close();
                        } else {
                            SharedPulsarClient.get(componentId, clientConf);
                        }
                    }
                } catch (Exception ignored) {
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(5, TimeUnit.SECONDS));
        executor.shutdown();
    }

    @Test
    public void testMultiTopicProducers() throws Exception {
        PulsarClientImpl mockClient = mock(PulsarClientImpl.class);
        SharedPulsarClient sharedClient = createMockedSharedClient("comp-producer", mockClient);

        @SuppressWarnings("unchecked")
        Producer<byte[]> producerTopic1 = mock(Producer.class);
        @SuppressWarnings("unchecked")
        Producer<byte[]> producerTopic2 = mock(Producer.class);

        ProducerConfigurationData conf1 = new ProducerConfigurationData();
        conf1.setTopicName("persistent://sample/ns1/topic-1");

        ProducerConfigurationData conf2 = new ProducerConfigurationData();
        conf2.setTopicName("persistent://sample/ns1/topic-2");

        when(mockClient.createProducerAsync(conf1)).thenReturn(CompletableFuture.completedFuture(producerTopic1));
        when(mockClient.createProducerAsync(conf2)).thenReturn(CompletableFuture.completedFuture(producerTopic2));

        Producer<byte[]> p1 = sharedClient.getSharedProducer(conf1);
        Producer<byte[]> p2 = sharedClient.getSharedProducer(conf2);

        assertNotSame(p1, p2);
        assertSame(p1, producerTopic1);
        assertSame(p2, producerTopic2);

        // Requesting topic-1 again should return cached instance without calling createProducerAsync
        Producer<byte[]> p1Cached = sharedClient.getSharedProducer(conf1);
        assertSame(p1, p1Cached);
        verify(mockClient, times(1)).createProducerAsync(conf1);
    }

    @Test
    public void testConsumerKeyTopicOrderingAndSubscriptionType() throws Exception {
        PulsarClientImpl mockClient = mock(PulsarClientImpl.class);
        SharedPulsarClient sharedClient = createMockedSharedClient("comp-consumer-order", mockClient);

        @SuppressWarnings("unchecked")
        Consumer<byte[]> consumerShared = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<byte[]> consumerFailover = mock(Consumer.class);

        // Conf 1: topic-1, topic-2 (Shared)
        ConsumerConfigurationData<byte[]> conf1 = new ConsumerConfigurationData<>();
        conf1.setTopicNames(new HashSet<>(Arrays.asList("topic-1", "topic-2")));
        conf1.setSubscriptionName("sub-order");
        conf1.setSubscriptionType(SubscriptionType.Shared);

        // Conf 2: topic-2, topic-1 in reverse order (Shared) -> should map to same cache key
        ConsumerConfigurationData<byte[]> conf2 = new ConsumerConfigurationData<>();
        conf2.setTopicNames(new HashSet<>(Arrays.asList("topic-2", "topic-1")));
        conf2.setSubscriptionName("sub-order");
        conf2.setSubscriptionType(SubscriptionType.Shared);

        // Conf 3: same topics but Failover -> should map to different cache key
        ConsumerConfigurationData<byte[]> conf3 = new ConsumerConfigurationData<>();
        conf3.setTopicNames(new HashSet<>(Arrays.asList("topic-1", "topic-2")));
        conf3.setSubscriptionName("sub-order");
        conf3.setSubscriptionType(SubscriptionType.Failover);

        when(mockClient.subscribeAsync(conf1)).thenReturn(CompletableFuture.completedFuture(consumerShared));
        when(mockClient.subscribeAsync(conf3)).thenReturn(CompletableFuture.completedFuture(consumerFailover));

        Consumer<byte[]> c1 = sharedClient.getSharedConsumer(conf1);
        Consumer<byte[]> c2 = sharedClient.getSharedConsumer(conf2);
        Consumer<byte[]> c3 = sharedClient.getSharedConsumer(conf3);

        assertSame(c1, c2, "Same topics in different order must return identical cached consumer");
        assertNotSame(c1, c3, "Different SubscriptionType must return different consumer");
        verify(mockClient, times(1)).subscribeAsync(conf1);
        verify(mockClient, times(1)).subscribeAsync(conf3);
    }

    @Test
    public void testMultiTopicReaders() throws Exception {
        PulsarClientImpl mockClient = mock(PulsarClientImpl.class);
        SharedPulsarClient sharedClient = createMockedSharedClient("comp-reader", mockClient);

        @SuppressWarnings("unchecked")
        Reader<byte[]> readerTopic1 = mock(Reader.class);
        @SuppressWarnings("unchecked")
        Reader<byte[]> readerTopic2 = mock(Reader.class);

        ReaderConfigurationData<byte[]> conf1 = new ReaderConfigurationData<>();
        conf1.setTopicName("persistent://sample/ns1/topic-1");
        conf1.setReaderName("reader-1");

        ReaderConfigurationData<byte[]> conf2 = new ReaderConfigurationData<>();
        conf2.setTopicName("persistent://sample/ns1/topic-2");
        conf2.setReaderName("reader-1");

        when(mockClient.createReaderAsync(conf1)).thenReturn(CompletableFuture.completedFuture(readerTopic1));
        when(mockClient.createReaderAsync(conf2)).thenReturn(CompletableFuture.completedFuture(readerTopic2));

        Reader<byte[]> r1 = sharedClient.getSharedReader(conf1);
        Reader<byte[]> r2 = sharedClient.getSharedReader(conf2);

        assertNotSame(r1, r2);
        assertSame(r1, readerTopic1);
        assertSame(r2, readerTopic2);

        // Requesting topic-1 again should return cached instance
        Reader<byte[]> r1Cached = sharedClient.getSharedReader(conf1);
        assertSame(r1, r1Cached);
        verify(mockClient, times(1)).createReaderAsync(conf1);
    }

    @Test
    public void testCompletionExceptionUnwrapping() throws Exception {
        PulsarClientImpl mockClient = mock(PulsarClientImpl.class);
        SharedPulsarClient sharedClient = createMockedSharedClient("comp-err", mockClient);

        ProducerConfigurationData conf = new ProducerConfigurationData();
        conf.setTopicName("persistent://sample/ns1/err-topic");

        CompletableFuture<Producer<byte[]>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new CompletionException(new RuntimeException("Socket timeout")));
        when(mockClient.createProducerAsync(conf)).thenReturn(failedFuture);

        try {
            sharedClient.getSharedProducer(conf);
            fail("Expected PulsarClientException");
        } catch (PulsarClientException e) {
            assertTrue(e.getMessage().contains("Socket timeout") || e.getCause() instanceof RuntimeException);
        }
    }

    @Test
    public void testClearInstances() {
        SharedPulsarClient.clearInstances();
        assertEquals(SharedPulsarClient.getInstanceCount(), 0);
    }
}

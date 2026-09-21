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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.pulsar.PulsarSpout.SpoutConsumer;
import org.apache.heron.pulsar.PulsarSpout.SpoutReader;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

public class PulsarSpoutTest {

    @BeforeMethod
    public void setUp() {
        SharedPulsarClient.clearInstances();
    }

    @AfterMethod
    public void tearDown() {
        SharedPulsarClient.clearInstances();
    }

    @Test
    public void testAckFailedMessage() throws Exception {
        PulsarSpoutConfiguration conf = new PulsarSpoutConfiguration();
        conf.setServiceUrl("http://localhost:8080");
        conf.setSubscriptionName("sub1");
        conf.setTopic("persistent://prop/ns1/topic1");
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        conf.setMessageToValuesMapper(new MessageToValuesMapper() {
            @Override
            public Values toValues(Message<byte[]> msg) {
                return null;
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }
        });

        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarSpout spout = Mockito.spy(new PulsarSpout(conf, builder));

        MessageImpl<byte[]> msg = new MessageImpl<>(conf.getTopic(), "1:1", Maps.newHashMap(),
                                                    new byte[0], Schema.BYTES, new MessageMetadata());
        Consumer<byte[]> consumer = mock(Consumer.class);
        SpoutConsumer spoutConsumer = new SpoutConsumer(consumer);
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.complete(null);
        doReturn(future).when(consumer).acknowledgeAsync(any(Message.class));
        Field consField = PulsarSpout.class.getDeclaredField("consumer");
        consField.setAccessible(true);
        consField.set(spout, spoutConsumer);

        spout.fail(msg);
        spout.ack(msg);
        spout.emitNextAvailableTuple();
        verify(consumer, atLeast(1)).receive(anyInt(), any());
    }

    @Test
    public void testAckFailureMetric() throws Exception {
        PulsarSpoutConfiguration conf = new PulsarSpoutConfiguration();
        conf.setServiceUrl("http://localhost:8080");
        conf.setSubscriptionName("sub1");
        conf.setTopic("persistent://prop/ns1/topic1");
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        conf.setMessageToValuesMapper(new MessageToValuesMapper() {
            @Override
            public Values toValues(Message<byte[]> msg) {
                return null;
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }
        });

        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarSpout spout = Mockito.spy(new PulsarSpout(conf, builder));

        MessageImpl<byte[]> msg = new MessageImpl<>(conf.getTopic(), "1:1", Maps.newHashMap(),
                                                    new byte[0], Schema.BYTES, new MessageMetadata());
        Consumer<byte[]> consumer = mock(Consumer.class);
        SpoutConsumer spoutConsumer = new SpoutConsumer(consumer);
        CompletableFuture<Void> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new org.apache.pulsar.client.api.PulsarClientException("Ack failed"));
        doReturn(failedFuture).when(consumer).acknowledgeAsync(any(Message.class));

        Field consField = PulsarSpout.class.getDeclaredField("consumer");
        consField.setAccessible(true);
        consField.set(spout, spoutConsumer);

        assertEquals(spout.getMetrics().get(PulsarSpout.NO_OF_FAILED_ACKS), 0L);
        spout.ack(msg);
        assertEquals(spout.getMetrics().get(PulsarSpout.NO_OF_FAILED_ACKS), 1L);

        Map<String, Object> metrics = spout.getValueAndReset();
        assertEquals(metrics.get(PulsarSpout.NO_OF_FAILED_ACKS), 1L);
        assertEquals(spout.getMetrics().get(PulsarSpout.NO_OF_FAILED_ACKS), 0L);
    }

    @Test
    public void testAckNullFuture() throws Exception {
        PulsarSpoutConfiguration conf = new PulsarSpoutConfiguration();
        conf.setServiceUrl("http://localhost:8080");
        conf.setSubscriptionName("sub1");
        conf.setTopic("persistent://prop/ns1/topic1");
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        conf.setMessageToValuesMapper(new MessageToValuesMapper() {
            @Override
            public Values toValues(Message<byte[]> msg) {
                return null;
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }
        });

        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarSpout spout = Mockito.spy(new PulsarSpout(conf, builder));

        MessageImpl<byte[]> msg = new MessageImpl<>(conf.getTopic(), "1:1", Maps.newHashMap(),
                                                    new byte[0], Schema.BYTES, new MessageMetadata());
        PulsarSpoutConsumer mockSpoutConsumer = mock(PulsarSpoutConsumer.class);
        doReturn(null).when(mockSpoutConsumer).acknowledgeAsync(any(Message.class));

        Field consField = PulsarSpout.class.getDeclaredField("consumer");
        consField.setAccessible(true);
        consField.set(spout, mockSpoutConsumer);

        spout.ack(msg);
        assertEquals(spout.getMetrics().get(PulsarSpout.NO_OF_FAILED_ACKS), 0L);
    }

    @Test
    public void testAckAsyncDelayedFailure() throws Exception {
        PulsarSpoutConfiguration conf = new PulsarSpoutConfiguration();
        conf.setServiceUrl("http://localhost:8080");
        conf.setSubscriptionName("sub1");
        conf.setTopic("persistent://prop/ns1/topic1");
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        conf.setMessageToValuesMapper(new MessageToValuesMapper() {
            @Override
            public Values toValues(Message<byte[]> msg) {
                return null;
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }
        });

        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarSpout spout = Mockito.spy(new PulsarSpout(conf, builder));

        MessageImpl<byte[]> msg = new MessageImpl<>(conf.getTopic(), "1:1", Maps.newHashMap(),
                                                    new byte[0], Schema.BYTES, new MessageMetadata());
        Consumer<byte[]> consumer = mock(Consumer.class);
        SpoutConsumer spoutConsumer = new SpoutConsumer(consumer);
        CompletableFuture<Void> pendingFuture = new CompletableFuture<>();
        doReturn(pendingFuture).when(consumer).acknowledgeAsync(any(Message.class));

        Field consField = PulsarSpout.class.getDeclaredField("consumer");
        consField.setAccessible(true);
        consField.set(spout, spoutConsumer);

        spout.ack(msg);
        assertEquals(spout.getMetrics().get(PulsarSpout.NO_OF_FAILED_ACKS), 0L);

        // Complete exceptionally asynchronously
        pendingFuture.completeExceptionally(new org.apache.pulsar.client.api.PulsarClientException("Async ack failed"));
        assertEquals(spout.getMetrics().get(PulsarSpout.NO_OF_FAILED_ACKS), 1L);
    }

    @Test
    public void testSpoutReaderAck() throws Exception {
        PulsarSpoutConfiguration conf = new PulsarSpoutConfiguration();
        conf.setServiceUrl("http://localhost:8080");
        conf.setSubscriptionName("sub1");
        conf.setTopic("persistent://prop/ns1/topic1");
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        conf.setMessageToValuesMapper(new MessageToValuesMapper() {
            @Override
            public Values toValues(Message<byte[]> msg) {
                return null;
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }
        });

        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarSpout spout = Mockito.spy(new PulsarSpout(conf, builder));

        MessageImpl<byte[]> msg = new MessageImpl<>(conf.getTopic(), "1:1", Maps.newHashMap(),
                                                    new byte[0], Schema.BYTES, new MessageMetadata());
        @SuppressWarnings("unchecked")
        Reader<byte[]> reader = mock(Reader.class);
        SpoutReader spoutReader = new SpoutReader(reader);

        Field consField = PulsarSpout.class.getDeclaredField("consumer");
        consField.setAccessible(true);
        consField.set(spout, spoutReader);

        spout.ack(msg);
        assertEquals(spout.getMetrics().get(PulsarSpout.NO_OF_FAILED_ACKS), 0L);
    }

    @Test
    public void testFailMessageRetryAndExhaustion() throws Exception {
        PulsarSpoutConfiguration conf = new PulsarSpoutConfiguration();
        conf.setServiceUrl("http://localhost:8080");
        conf.setSubscriptionName("sub1");
        conf.setTopic("persistent://prop/ns1/topic1");
        conf.setMaxFailedRetries(2);
        conf.setFailedRetriesTimeout(60, TimeUnit.SECONDS);
        conf.setMessageToValuesMapper(new MessageToValuesMapper() {
            @Override
            public Values toValues(Message<byte[]> msg) {
                return new Values("val");
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }
        });

        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarSpout spout = Mockito.spy(new PulsarSpout(conf, builder));

        Consumer<byte[]> consumer = mock(Consumer.class);
        SpoutConsumer spoutConsumer = new SpoutConsumer(consumer);
        when(consumer.acknowledgeAsync(any(Message.class))).thenReturn(CompletableFuture.completedFuture(null));

        Field consField = PulsarSpout.class.getDeclaredField("consumer");
        consField.setAccessible(true);
        consField.set(spout, spoutConsumer);

        MessageImpl<byte[]> msg = new MessageImpl<>(conf.getTopic(), "1:1", Maps.newHashMap(),
                new byte[0], Schema.BYTES, new MessageMetadata());

        // Fail retry 1
        spout.fail(msg);
        assertEquals(spout.getMetrics().get(PulsarSpout.NO_OF_MESSAGES_FAILED), 1L);

        // Fail retry 2
        spout.fail(msg);
        assertEquals(spout.getMetrics().get(PulsarSpout.NO_OF_MESSAGES_FAILED), 2L);

        // Fail retry 3 -> reaches limit 2, drops and calls ack
        spout.fail(msg);
        verify(consumer, times(1)).acknowledgeAsync(eq(msg));
    }

    @Test
    public void testCloseWithAutoUnsubscribe() throws Exception {
        PulsarSpoutConfiguration conf = new PulsarSpoutConfiguration();
        conf.setServiceUrl("http://localhost:8080");
        conf.setSubscriptionName("sub1");
        conf.setTopic("persistent://prop/ns1/topic1");
        conf.setAutoUnsubscribe(true);
        conf.setSharedConsumerEnabled(false);
        conf.setMessageToValuesMapper(mock(MessageToValuesMapper.class));

        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarSpout spout = Mockito.spy(new PulsarSpout(conf, builder));

        Consumer<byte[]> consumer = mock(Consumer.class);
        SpoutConsumer spoutConsumer = new SpoutConsumer(consumer);

        Field consField = PulsarSpout.class.getDeclaredField("consumer");
        consField.setAccessible(true);
        consField.set(spout, spoutConsumer);

        spout.close();

        verify(consumer, times(1)).unsubscribe();
        verify(consumer, times(1)).close();
    }

    @Test
    public void testReaderPatternException() {
        PulsarSpoutConfiguration conf = new PulsarSpoutConfiguration();
        conf.setServiceUrl("http://localhost:8080");
        conf.setSubscriptionName("sub1");
        conf.setTopicPattern(Pattern.compile("persistent://prop/ns1/.*"));
        conf.setDurableSubscription(false);
        conf.setMessageToValuesMapper(mock(MessageToValuesMapper.class));

        PulsarSpout spout = new PulsarSpout(conf);
        expectThrows(IllegalStateException.class, spout::newReaderConfiguration);
    }

    @Test
    public void testNonBlockingBackoffBypass() throws Exception {
        PulsarSpoutConfiguration conf = new PulsarSpoutConfiguration();
        conf.setServiceUrl("http://localhost:8080");
        conf.setSubscriptionName("sub1");
        conf.setTopic("persistent://prop/ns1/topic1");
        conf.setMaxFailedRetries(5);
        conf.setFailedRetriesTimeout(60, TimeUnit.SECONDS);
        conf.setMessageToValuesMapper(new MessageToValuesMapper() {
            @Override
            public Values toValues(Message<byte[]> msg) {
                return new Values(new String(msg.getData()));
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }
        });

        PulsarSpout spout = new PulsarSpout(conf);
        TopologyContext context = mock(TopologyContext.class);
        when(context.getThisComponentId()).thenReturn("comp-backoff");
        when(context.getThisTaskId()).thenReturn(1);
        when(context.getThisTaskIndex()).thenReturn(0);

        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        Consumer<byte[]> consumer = mock(Consumer.class);
        SpoutConsumer spoutConsumer = new SpoutConsumer(consumer);

        Field consField = PulsarSpout.class.getDeclaredField("consumer");
        consField.setAccessible(true);
        consField.set(spout, spoutConsumer);

        Field colField = PulsarSpout.class.getDeclaredField("collector");
        colField.setAccessible(true);
        colField.set(spout, collector);

        // Put a message in retry queue so it's in backoff
        MessageImpl<byte[]> failedMsg = new MessageImpl<>(conf.getTopic(), "1:1", Maps.newHashMap(),
                "failed-val".getBytes(), Schema.BYTES, new MessageMetadata());
        spout.fail(failedMsg);

        // When emitNextAvailableTuple runs, emitFailedMessage() should return false (due to backoff)
        // and consumer.receive() should be invoked to receive live messages
        MessageImpl<byte[]> liveMsg = new MessageImpl<>(conf.getTopic(), "1:2", Maps.newHashMap(),
                "live-val".getBytes(), Schema.BYTES, new MessageMetadata());
        when(consumer.receive(anyInt(), any())).thenReturn(liveMsg);

        spout.emitNextAvailableTuple();

        // Verify live message was emitted
        verify(collector, times(1)).emit(any(Values.class), eq(liveMsg));
    }

    @Test
    public void testPulsarTuple() throws Exception {
        testPulsarSpout(true);
    }

    @Test
    public void testPulsarSpout() throws Exception {
        testPulsarSpout(false);
    }

    public void testPulsarSpout(boolean pulsarTuple) throws Exception {
        PulsarSpoutConfiguration conf = new PulsarSpoutConfiguration();
        conf.setServiceUrl("http://localhost:8080");
        conf.setSubscriptionName("sub1");
        conf.setTopic("persistent://prop/ns1/topic1");
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        conf.setSharedConsumerEnabled(true);
        AtomicBoolean called = new AtomicBoolean(false);
        conf.setMessageToValuesMapper(new MessageToValuesMapper() {
            @Override
            public Values toValues(Message<byte[]> msg) {
                called.set(true);
                if ("message to be dropped".equals(new String(msg.getData()))) {
                    return null;
                }
                String val = new String(msg.getData());
                if (val.startsWith("stream:")) {
                    String stream = val.split(":")[1];
                    return new PulsarTuple(stream, val);
                }
                return new Values(val);
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }

        });

        String msgContent = pulsarTuple ? "stream:pstream" : "test";

        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarSpout spout = Mockito.spy(new PulsarSpout(conf, builder));
        TopologyContext context = mock(TopologyContext.class);
        final String componentId = "test-component-id";
        doReturn(componentId).when(context).getThisComponentId();
        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        Map<String, Object> config = new HashMap<>();

        SharedPulsarClient client = mock(SharedPulsarClient.class);
        Consumer<byte[]> consumer = mock(Consumer.class);
        when(client.getSharedConsumer(any())).thenReturn(consumer);

        Field field = SharedPulsarClient.class.getDeclaredField("instances");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<String, SharedPulsarClient> instances = (ConcurrentMap<String, SharedPulsarClient>) field
                .get(SharedPulsarClient.class);
        instances.put(componentId, client);

        MessageImpl<byte[]> msg = new MessageImpl<>(conf.getTopic(), "1:1", Maps.newHashMap(),
                                          msgContent.getBytes(), Schema.BYTES, new MessageMetadata());
        when(consumer.receive(anyInt(), any())).thenReturn(msg);

        spout.open(config, context, collector);
        spout.emitNextAvailableTuple();

        assertTrue(called.get());
        verify(consumer, atLeast(1)).receive(anyInt(), any());
        ArgumentCaptor<Values> capt = ArgumentCaptor.forClass(Values.class);
        if (pulsarTuple) {
            verify(collector, times(1)).emit(eq("pstream"), capt.capture(), eq(msg));
        } else {
            verify(collector, times(1)).emit(capt.capture(), eq(msg));
        }
        Values vals = capt.getValue();
        assertEquals(msgContent, vals.get(0));
    }
}

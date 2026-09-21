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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.expectThrows;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.heron.api.Constants;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PulsarBoltTest {

    private PulsarBoltConfiguration boltConf;
    private TupleToMessageMapper mapper;
    private Producer<byte[]> producer;
    private SharedPulsarClient sharedClient;
    private TopologyContext context;
    private OutputCollector collector;

    @BeforeMethod
    public void setUp() {
        SharedPulsarClient.clearInstances();
        boltConf = new PulsarBoltConfiguration();
        boltConf.setServiceUrl("pulsar://localhost:6650");
        boltConf.setTopic("persistent://sample/ns1/test-topic");
        mapper = mock(TupleToMessageMapper.class);
        boltConf.setTupleToMessageMapper(mapper);

        producer = mock(Producer.class);
        sharedClient = mock(SharedPulsarClient.class);
        context = mock(TopologyContext.class);
        collector = mock(OutputCollector.class);

        when(context.getThisComponentId()).thenReturn("pulsar-bolt-comp");
        when(context.getThisTaskId()).thenReturn(1);
        when(context.getThisTaskIndex()).thenReturn(0);
    }

    @AfterMethod
    public void tearDown() {
        SharedPulsarClient.clearInstances();
    }

    @Test
    public void testConstructorNullTopicValidation() {
        PulsarBoltConfiguration invalidConf = new PulsarBoltConfiguration();
        invalidConf.setServiceUrl("pulsar://localhost:6650");
        invalidConf.setTupleToMessageMapper(mock(TupleToMessageMapper.class));
        // Topic left as null
        expectThrows(NullPointerException.class, () -> new PulsarBolt(invalidConf));
    }

    @Test
    public void testPrepareAndMetricsRegistration() throws Exception {
        when(sharedClient.getSharedProducer(any())).thenReturn(producer);

        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarBolt bolt = new PulsarBolt(boltConf, builder);

        Field clientField = SharedPulsarClient.class.getDeclaredField("instances");
        clientField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, SharedPulsarClient> instances = (Map<String, SharedPulsarClient>) clientField.get(null);
        instances.put("pulsar-bolt-comp", sharedClient);

        bolt.prepare(new HashMap<>(), context, collector);

        verify(context, times(1)).registerMetric(eq("PulsarBolt/pulsar-bolt-comp-0"), eq(bolt), eq(60));
        verify(sharedClient, times(1)).getSharedProducer(any());
    }

    @Test
    public void testExecuteTickTuple() throws Exception {
        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarBolt bolt = new PulsarBolt(boltConf, builder);

        Tuple tickTuple = mock(Tuple.class);
        when(tickTuple.getSourceStreamId()).thenReturn(Constants.SYSTEM_TICK_STREAM_ID);
        when(tickTuple.getSourceComponent()).thenReturn(Constants.SYSTEM_COMPONENT_ID);

        Field colField = PulsarBolt.class.getDeclaredField("collector");
        colField.setAccessible(true);
        colField.set(bolt, collector);

        bolt.execute(tickTuple);

        verify(collector, times(1)).ack(tickTuple);
        verify(mapper, never()).toMessage(any(), any());
    }

    @Test
    public void testExecuteNullProducer() throws Exception {
        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarBolt bolt = new PulsarBolt(boltConf, builder);

        Tuple input = mock(Tuple.class);
        when(input.getSourceStreamId()).thenReturn("default");
        when(input.getSourceComponent()).thenReturn("source-comp");

        Field colField = PulsarBolt.class.getDeclaredField("collector");
        colField.setAccessible(true);
        colField.set(bolt, collector);

        bolt.execute(input);

        verify(collector, times(1)).reportError(any(IllegalStateException.class));
        verify(collector, times(1)).fail(input);
        assertEquals(bolt.getMetrics().get(PulsarBolt.NO_OF_MESSAGES_FAILED), 1L);
    }

    @Test
    public void testExecuteNullMessageFromMapper() throws Exception {
        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarBolt bolt = new PulsarBolt(boltConf, builder);

        Field prodField = PulsarBolt.class.getDeclaredField("producer");
        prodField.setAccessible(true);
        prodField.set(bolt, producer);

        Field colField = PulsarBolt.class.getDeclaredField("collector");
        colField.setAccessible(true);
        colField.set(bolt, collector);

        @SuppressWarnings("unchecked")
        TypedMessageBuilder<byte[]> mockMsgBuilder = mock(TypedMessageBuilder.class);
        when(producer.newMessage()).thenReturn(mockMsgBuilder);
        when(mapper.toMessage(mockMsgBuilder, null)).thenReturn(null);

        Tuple input = mock(Tuple.class);
        when(input.getSourceStreamId()).thenReturn("default");
        when(input.getSourceComponent()).thenReturn("source-comp");

        when(mapper.toMessage(eq(mockMsgBuilder), eq(input))).thenReturn(null);

        bolt.execute(input);

        verify(collector, times(1)).ack(input);
        verify(mockMsgBuilder, never()).sendAsync();
    }

    @Test
    public void testExecuteSendAsyncSuccess() throws Exception {
        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarBolt bolt = new PulsarBolt(boltConf, builder);

        Field prodField = PulsarBolt.class.getDeclaredField("producer");
        prodField.setAccessible(true);
        prodField.set(bolt, producer);

        Field colField = PulsarBolt.class.getDeclaredField("collector");
        colField.setAccessible(true);
        colField.set(bolt, collector);

        @SuppressWarnings("unchecked")
        TypedMessageBuilderImpl<byte[]> mockMsgBuilder = mock(TypedMessageBuilderImpl.class);
        when(producer.newMessage()).thenReturn(mockMsgBuilder);

        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[]{1, 2, 3, 4});
        when(mockMsgBuilder.getContent()).thenReturn(byteBuffer);

        MessageId mockMsgId = mock(MessageId.class);
        CompletableFuture<MessageId> future = CompletableFuture.completedFuture(mockMsgId);
        when(mockMsgBuilder.sendAsync()).thenReturn(future);

        Tuple input = mock(Tuple.class);
        when(input.getSourceStreamId()).thenReturn("default");
        when(input.getSourceComponent()).thenReturn("source-comp");
        when(mapper.toMessage(eq(mockMsgBuilder), eq(input))).thenReturn(mockMsgBuilder);

        bolt.execute(input);

        verify(collector, times(1)).ack(input);
        Map<String, Object> metrics = bolt.getMetrics();
        assertEquals(metrics.get(PulsarBolt.NO_OF_MESSAGES_SENT), 1L);
        assertEquals(metrics.get(PulsarBolt.NO_OF_MESSAGES_FAILED), 0L);
    }

    @Test
    public void testExecuteSendAsyncFailure() throws Exception {
        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarBolt bolt = new PulsarBolt(boltConf, builder);

        Field prodField = PulsarBolt.class.getDeclaredField("producer");
        prodField.setAccessible(true);
        prodField.set(bolt, producer);

        Field colField = PulsarBolt.class.getDeclaredField("collector");
        colField.setAccessible(true);
        colField.set(bolt, collector);

        @SuppressWarnings("unchecked")
        TypedMessageBuilder<byte[]> mockMsgBuilder = mock(TypedMessageBuilder.class);
        when(producer.newMessage()).thenReturn(mockMsgBuilder);

        CompletableFuture<MessageId> future = new CompletableFuture<>();
        PulsarClientException testEx = new PulsarClientException("Producer send failure");
        future.completeExceptionally(testEx);
        when(mockMsgBuilder.sendAsync()).thenReturn(future);

        Tuple input = mock(Tuple.class);
        when(input.getSourceStreamId()).thenReturn("default");
        when(input.getSourceComponent()).thenReturn("source-comp");
        when(mapper.toMessage(eq(mockMsgBuilder), eq(input))).thenReturn(mockMsgBuilder);

        bolt.execute(input);

        verify(collector, times(1)).reportError(testEx);
        verify(collector, times(1)).fail(input);
        assertEquals(bolt.getMetrics().get(PulsarBolt.NO_OF_MESSAGES_SENT), 0L);
        assertEquals(bolt.getMetrics().get(PulsarBolt.NO_OF_MESSAGES_FAILED), 1L);
    }

    @Test
    public void testExecuteMapperThrowsException() throws Exception {
        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarBolt bolt = new PulsarBolt(boltConf, builder);

        Field prodField = PulsarBolt.class.getDeclaredField("producer");
        prodField.setAccessible(true);
        prodField.set(bolt, producer);

        Field colField = PulsarBolt.class.getDeclaredField("collector");
        colField.setAccessible(true);
        colField.set(bolt, collector);

        @SuppressWarnings("unchecked")
        TypedMessageBuilder<byte[]> mockMsgBuilder = mock(TypedMessageBuilder.class);
        when(producer.newMessage()).thenReturn(mockMsgBuilder);

        RuntimeException mapperEx = new RuntimeException("Serialization failure in mapper");
        Tuple input = mock(Tuple.class);
        when(input.getSourceStreamId()).thenReturn("default");
        when(input.getSourceComponent()).thenReturn("source-comp");
        when(mapper.toMessage(eq(mockMsgBuilder), eq(input))).thenThrow(mapperEx);

        bolt.execute(input);

        verify(collector, times(1)).reportError(mapperEx);
        verify(collector, times(1)).fail(input);
        assertEquals(bolt.getMetrics().get(PulsarBolt.NO_OF_MESSAGES_FAILED), 1L);
    }

    @Test
    public void testMetricsGetValueAndReset() throws Exception {
        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarBolt bolt = new PulsarBolt(boltConf, builder);

        Field prodField = PulsarBolt.class.getDeclaredField("producer");
        prodField.setAccessible(true);
        prodField.set(bolt, producer);

        Field colField = PulsarBolt.class.getDeclaredField("collector");
        colField.setAccessible(true);
        colField.set(bolt, collector);

        @SuppressWarnings("unchecked")
        TypedMessageBuilderImpl<byte[]> mockMsgBuilder = mock(TypedMessageBuilderImpl.class);
        when(producer.newMessage()).thenReturn(mockMsgBuilder);
        when(mockMsgBuilder.getContent()).thenReturn(ByteBuffer.wrap(new byte[10]));
        when(mockMsgBuilder.sendAsync()).thenReturn(CompletableFuture.completedFuture(mock(MessageId.class)));

        Tuple input = mock(Tuple.class);
        when(input.getSourceStreamId()).thenReturn("default");
        when(input.getSourceComponent()).thenReturn("source-comp");
        when(mapper.toMessage(eq(mockMsgBuilder), eq(input))).thenReturn(mockMsgBuilder);

        bolt.execute(input);
        bolt.execute(input);

        Map<String, Object> metrics = bolt.getValueAndReset();
        assertEquals(metrics.get(PulsarBolt.NO_OF_MESSAGES_SENT), 2L);
        assertEquals(bolt.getMetrics().get(PulsarBolt.NO_OF_MESSAGES_SENT), 0L);
    }

    @Test
    public void testCloseAndFlush() throws Exception {
        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarBolt bolt = new PulsarBolt(boltConf, builder);

        Field prodField = PulsarBolt.class.getDeclaredField("producer");
        prodField.setAccessible(true);
        prodField.set(bolt, producer);

        Field clientField = PulsarBolt.class.getDeclaredField("sharedPulsarClient");
        clientField.setAccessible(true);
        clientField.set(bolt, sharedClient);

        bolt.close();

        verify(producer, times(1)).flush();
        verify(sharedClient, times(1)).close();
    }

    @Test
    public void testCloseFlushExceptionHandledGracefully() throws Exception {
        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarBolt bolt = new PulsarBolt(boltConf, builder);

        Field prodField = PulsarBolt.class.getDeclaredField("producer");
        prodField.setAccessible(true);
        prodField.set(bolt, producer);

        Field clientField = PulsarBolt.class.getDeclaredField("sharedPulsarClient");
        clientField.setAccessible(true);
        clientField.set(bolt, sharedClient);

        doThrow(new PulsarClientException("Flush timeout")).when(producer).flush();

        bolt.close();

        verify(sharedClient, times(1)).close();
    }

    @Test
    public void testCleanupDelegatesToClose() throws Exception {
        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarBolt bolt = new PulsarBolt(boltConf, builder);

        Field prodField = PulsarBolt.class.getDeclaredField("producer");
        prodField.setAccessible(true);
        prodField.set(bolt, producer);

        Field clientField = PulsarBolt.class.getDeclaredField("sharedPulsarClient");
        clientField.setAccessible(true);
        clientField.set(bolt, sharedClient);

        bolt.cleanup();

        verify(producer, times(1)).flush();
        verify(sharedClient, times(1)).close();
    }

    @Test
    public void testDeclareOutputFields() {
        ClientBuilder builder = spy(new ClientBuilderImpl());
        PulsarBolt bolt = new PulsarBolt(boltConf, builder);

        OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
        bolt.declareOutputFields(declarer);

        verify(mapper, times(1)).declareOutputFields(declarer);
    }
}

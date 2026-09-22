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

package org.apache.heron.kafka.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.heron.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KafkaBoltTest {

    @Test
    public void testKafkaBoltAsyncSendSuccess() {
        Producer<String, String> mockProducer = Mockito.mock(Producer.class);
        OutputCollector mockCollector = Mockito.mock(OutputCollector.class);
        TopologyContext mockContext = Mockito.mock(TopologyContext.class);

        KafkaBolt<String, String> bolt = new KafkaBolt<String, String>() {
            @Override
            protected Producer<String, String> mkProducer(Properties props) {
                return mockProducer;
            }
        };

        bolt.withTopicSelector(new DefaultTopicSelector("test-topic"))
            .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>("key", "message"));

        Map<String, Object> topoConf = new HashMap<>();
        bolt.prepare(topoConf, mockContext, mockCollector);

        Tuple tuple = Mockito.mock(Tuple.class);
        Mockito.when(tuple.contains("key")).thenReturn(true);
        Mockito.when(tuple.getValueByField("key")).thenReturn("k1");
        Mockito.when(tuple.getValueByField("message")).thenReturn("v1");

        Future<RecordMetadata> future = CompletableFuture.completedFuture(null);
        ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
        Mockito.when(mockProducer.send(Mockito.any(ProducerRecord.class), callbackCaptor.capture())).thenReturn(future);

        bolt.execute(tuple);

        Mockito.verify(mockProducer).send(Mockito.any(ProducerRecord.class), Mockito.any(Callback.class));
        Callback callback = callbackCaptor.getValue();
        Assert.assertNotNull(callback);

        callback.onCompletion(null, null);
        Mockito.verify(mockCollector).ack(tuple);
    }

    @Test
    public void testKafkaBoltNullTopicSkips() {
        Producer<String, String> mockProducer = Mockito.mock(Producer.class);
        OutputCollector mockCollector = Mockito.mock(OutputCollector.class);
        TopologyContext mockContext = Mockito.mock(TopologyContext.class);

        KafkaBolt<String, String> bolt = new KafkaBolt<String, String>() {
            @Override
            protected Producer<String, String> mkProducer(Properties props) {
                return mockProducer;
            }
        };

        bolt.withTopicSelector(t -> null) // returns null topic
            .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>());

        bolt.prepare(new HashMap<>(), mockContext, mockCollector);

        Tuple tuple = Mockito.mock(Tuple.class);
        Mockito.when(tuple.contains("key")).thenReturn(false);
        Mockito.when(tuple.getValueByField("message")).thenReturn("v1");

        bolt.execute(tuple);
        Mockito.verify(mockCollector).ack(tuple);
        Mockito.verify(mockProducer, Mockito.never()).send(Mockito.any(), Mockito.any());
    }

    @Test
    public void testKafkaBoltFireAndForget() {
        Producer<String, String> mockProducer = Mockito.mock(Producer.class);
        OutputCollector mockCollector = Mockito.mock(OutputCollector.class);
        TopologyContext mockContext = Mockito.mock(TopologyContext.class);

        KafkaBolt<String, String> bolt = new KafkaBolt<String, String>() {
            @Override
            protected Producer<String, String> mkProducer(Properties props) {
                return mockProducer;
            }
        };

        bolt.withTopicSelector("test-topic");
        bolt.setFireAndForget(true);
        bolt.prepare(new HashMap<>(), mockContext, mockCollector);

        Tuple tuple = Mockito.mock(Tuple.class);
        Mockito.when(tuple.contains("key")).thenReturn(true);
        Mockito.when(tuple.getValueByField("key")).thenReturn("k1");
        Mockito.when(tuple.getValueByField("message")).thenReturn("v1");

        Mockito.when(mockProducer.send(Mockito.any(), Mockito.any())).thenReturn(CompletableFuture.completedFuture(null));

        bolt.execute(tuple);
        Mockito.verify(mockCollector).ack(tuple);
    }
}

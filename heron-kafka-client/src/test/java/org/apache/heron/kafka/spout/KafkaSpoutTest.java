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
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.kafka.spout.internal.ConsumerFactory;
import org.apache.heron.kafka.spout.subscription.TopicAssigner;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KafkaSpoutTest {

    @Test
    public void testSpoutOpenAndLifecycle() {
        KafkaSpoutConfig<String, String> config = KafkaSpoutConfig.builder("127.0.0.1:9092", "test-topic")
            .setProp(ConsumerConfig.GROUP_ID_CONFIG, "test-group")
            .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
            .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
            .build();

        @SuppressWarnings("unchecked")
        Consumer<String, String> mockConsumer = Mockito.mock(Consumer.class);
        Mockito.when(mockConsumer.poll(Mockito.anyLong())).thenReturn(ConsumerRecords.empty());
        Mockito.when(mockConsumer.assignment()).thenReturn(Collections.emptySet());

        ConsumerFactory<String, String> factory = props -> mockConsumer;
        TopicAssigner assigner = new TopicAssigner();

        KafkaSpout<String, String> spout = new KafkaSpout<>(config, factory, assigner);

        TopologyContext context = Mockito.mock(TopologyContext.class);
        SpoutOutputCollector collector = Mockito.mock(SpoutOutputCollector.class);
        Map<String, Object> conf = new HashMap<>();

        spout.open(conf, context, collector);
        spout.activate();
        spout.nextTuple();

        Assert.assertNotNull(spout.getKafkaOffsetMetric());

        // Test ack on non-existent or rebalanced partition doesn't throw NPE
        TopicPartition unassignedTp = new TopicPartition("test-topic", 99);
        KafkaSpoutMessageId msgId = new KafkaSpoutMessageId(unassignedTp, 123L);
        spout.ack(msgId);
        spout.fail(msgId);

        spout.deactivate();
        spout.close();
    }
}

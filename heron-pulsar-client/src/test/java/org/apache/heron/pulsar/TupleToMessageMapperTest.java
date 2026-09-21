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
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import java.util.Collections;
import java.util.Map;

import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.tuple.Tuple;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.testng.annotations.Test;

public class TupleToMessageMapperTest {

    @Test
    public void testDefaultToMessageNullHandling() {
        TupleToMessageMapper mapper = new TupleToMessageMapper() {
            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }
        };

        Tuple mockTuple = mock(Tuple.class);
        assertNull(mapper.toMessage(mockTuple));

        @SuppressWarnings("unchecked")
        TypedMessageBuilder<byte[]> mockBuilder = mock(TypedMessageBuilder.class);
        TypedMessageBuilder<byte[]> result = mapper.toMessage(mockBuilder, mockTuple);
        assertNull(result, "When toMessage(tuple) returns null, toMessage(builder, tuple) must return null without NPE");
        verify(mockBuilder, never()).value(any());
    }

    @Test
    public void testDefaultToMessageWithValidMessageAndKey() {
        byte[] payload = "test-payload".getBytes();
        Map<String, String> properties = Collections.singletonMap("k1", "v1");

        @SuppressWarnings("unchecked")
        Message<byte[]> mockMessage = mock(Message.class);
        when(mockMessage.getData()).thenReturn(payload);
        when(mockMessage.getProperties()).thenReturn(properties);
        when(mockMessage.hasKey()).thenReturn(true);
        when(mockMessage.getKey()).thenReturn("msg-key");

        TupleToMessageMapper mapper = new TupleToMessageMapper() {
            @Override
            public Message<byte[]> toMessage(Tuple tuple) {
                return mockMessage;
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }
        };

        Tuple mockTuple = mock(Tuple.class);
        @SuppressWarnings("unchecked")
        TypedMessageBuilder<byte[]> mockBuilder = mock(TypedMessageBuilder.class);
        when(mockBuilder.value(payload)).thenReturn(mockBuilder);
        when(mockBuilder.properties(properties)).thenReturn(mockBuilder);
        when(mockBuilder.key("msg-key")).thenReturn(mockBuilder);

        TypedMessageBuilder<byte[]> result = mapper.toMessage(mockBuilder, mockTuple);
        assertNotNull(result);
        assertSame(result, mockBuilder);

        verify(mockBuilder, times(1)).value(eq(payload));
        verify(mockBuilder, times(1)).properties(eq(properties));
        verify(mockBuilder, times(1)).key(eq("msg-key"));
    }

    @Test
    public void testDefaultToMessageWithoutKey() {
        byte[] payload = "payload-no-key".getBytes();

        @SuppressWarnings("unchecked")
        Message<byte[]> mockMessage = mock(Message.class);
        when(mockMessage.getData()).thenReturn(payload);
        when(mockMessage.getProperties()).thenReturn(Collections.emptyMap());
        when(mockMessage.hasKey()).thenReturn(false);

        TupleToMessageMapper mapper = new TupleToMessageMapper() {
            @Override
            public Message<byte[]> toMessage(Tuple tuple) {
                return mockMessage;
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }
        };

        Tuple mockTuple = mock(Tuple.class);
        @SuppressWarnings("unchecked")
        TypedMessageBuilder<byte[]> mockBuilder = mock(TypedMessageBuilder.class);
        when(mockBuilder.value(payload)).thenReturn(mockBuilder);
        when(mockBuilder.properties(anyMap())).thenReturn(mockBuilder);

        TypedMessageBuilder<byte[]> result = mapper.toMessage(mockBuilder, mockTuple);
        assertNotNull(result);
        assertSame(result, mockBuilder);

        verify(mockBuilder, times(1)).value(eq(payload));
        verify(mockBuilder, never()).key(anyString());
    }

    @Test
    public void testCustomToMessageBuilderOverride() {
        TupleToMessageMapper mapper = new TupleToMessageMapper() {
            @Override
            public TypedMessageBuilder<byte[]> toMessage(TypedMessageBuilder<byte[]> msgBuilder, Tuple tuple) {
                return msgBuilder.key("custom-key");
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer declarer) {
            }
        };

        Tuple mockTuple = mock(Tuple.class);
        @SuppressWarnings("unchecked")
        TypedMessageBuilder<byte[]> mockBuilder = mock(TypedMessageBuilder.class);
        when(mockBuilder.key("custom-key")).thenReturn(mockBuilder);

        TypedMessageBuilder<byte[]> result = mapper.toMessage(mockBuilder, mockTuple);
        assertSame(result, mockBuilder);
        verify(mockBuilder, times(1)).key("custom-key");
    }
}

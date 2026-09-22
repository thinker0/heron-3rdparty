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

package org.apache.heron.kafka.bolt.mapper;

import org.apache.heron.api.tuple.Tuple;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FieldNameBasedTupleToKafkaMapperTest {

    @Test
    public void testDefaultFieldMapping() {
        FieldNameBasedTupleToKafkaMapper<String, String> mapper = new FieldNameBasedTupleToKafkaMapper<>();
        Assert.assertEquals(mapper.getBoltKeyField(), "key");
        Assert.assertEquals(mapper.getBoltMessageField(), "message");

        Tuple tuple = Mockito.mock(Tuple.class);
        Mockito.when(tuple.contains("key")).thenReturn(true);
        Mockito.when(tuple.getValueByField("key")).thenReturn("k1");
        Mockito.when(tuple.getValueByField("message")).thenReturn("msg1");

        Assert.assertEquals(mapper.getKeyFromTuple(tuple), "k1");
        Assert.assertEquals(mapper.getMessageFromTuple(tuple), "msg1");
    }

    @Test
    public void testCustomFieldMapping() {
        FieldNameBasedTupleToKafkaMapper<String, String> mapper =
                new FieldNameBasedTupleToKafkaMapper<>("customKey", "customMsg");
        Assert.assertEquals(mapper.getBoltKeyField(), "customKey");
        Assert.assertEquals(mapper.getBoltMessageField(), "customMsg");

        Tuple tuple = Mockito.mock(Tuple.class);
        Mockito.when(tuple.contains("customKey")).thenReturn(false);
        Mockito.when(tuple.getValueByField("customMsg")).thenReturn("customPayload");

        Assert.assertNull(mapper.getKeyFromTuple(tuple));
        Assert.assertEquals(mapper.getMessageFromTuple(tuple), "customPayload");
    }
}

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

package org.apache.heron.hdfs.bolt.format;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.heron.api.tuple.Tuple;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DefaultSequenceFormatTest {

    @Test
    public void testDefaultSequenceFormat() {
        DefaultSequenceFormat format = new DefaultSequenceFormat("offsetKey", "msgValue");
        Assert.assertEquals(format.keyClass(), LongWritable.class);
        Assert.assertEquals(format.valueClass(), Text.class);

        Tuple tuple = Mockito.mock(Tuple.class);
        Mockito.when(tuple.getLongByField("offsetKey")).thenReturn(12345L);
        Mockito.when(tuple.getStringByField("msgValue")).thenReturn("test-payload");

        Writable key = format.key(tuple);
        Assert.assertTrue(key instanceof LongWritable);
        Assert.assertEquals(((LongWritable) key).get(), 12345L);

        Writable value = format.value(tuple);
        Assert.assertTrue(value instanceof Text);
        Assert.assertEquals(((Text) value).toString(), "test-payload");
    }
}

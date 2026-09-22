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

import java.nio.charset.StandardCharsets;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DelimitedRecordFormatTest {

    @Test
    public void testDelimitedRecordFormatDefault() {
        DelimitedRecordFormat format = new DelimitedRecordFormat();
        Tuple tuple = Mockito.mock(Tuple.class);
        Fields fields = new Fields("f1", "f2");

        Mockito.when(tuple.getFields()).thenReturn(fields);
        Mockito.when(tuple.getValueByField("f1")).thenReturn("val1");
        Mockito.when(tuple.getValueByField("f2")).thenReturn("val2");

        byte[] bytes = format.format(tuple);
        String result = new String(bytes, StandardCharsets.UTF_8);
        Assert.assertEquals(result, "val1,val2\n");
    }

    @Test
    public void testDelimitedRecordFormatCustomDelimitersAndFields() {
        DelimitedRecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("\t")
                .withRecordDelimiter("\r\n")
                .withFields(new Fields("id", "name"));

        Tuple tuple = Mockito.mock(Tuple.class);
        Mockito.when(tuple.getValueByField("id")).thenReturn(100);
        Mockito.when(tuple.getValueByField("name")).thenReturn("heron");

        byte[] bytes = format.format(tuple);
        String result = new String(bytes, StandardCharsets.UTF_8);
        Assert.assertEquals(result, "100\theron\r\n");
    }
}

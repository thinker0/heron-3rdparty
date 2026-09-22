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

import org.testng.Assert;
import org.testng.annotations.Test;

public class KafkaTupleTest {

    @Test
    public void testRoutedTo() {
        KafkaTuple tuple = new KafkaTuple("val1", "val2");
        Assert.assertNull(tuple.getStream());

        tuple.routedTo("custom-stream");
        Assert.assertEquals(tuple.getStream(), "custom-stream");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testRoutedToThrowsOnDuplicate() {
        KafkaTuple tuple = new KafkaTuple("val1");
        tuple.routedTo("stream1");
        tuple.routedTo("stream2"); // should throw IllegalStateException
    }
}

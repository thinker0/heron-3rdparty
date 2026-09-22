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

package org.apache.heron.topology.base;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.heron.api.Constants;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BaseTickTupleAwareRichBoltTest {

    @Test
    public void testTickTupleDispatch() {
        AtomicBoolean tickHandled = new AtomicBoolean(false);
        AtomicBoolean tupleProcessed = new AtomicBoolean(false);

        BaseTickTupleAwareRichBolt bolt = new BaseTickTupleAwareRichBolt() {
            @Override
            public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
            }

            @Override
            public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            }

            @Override
            protected void onTickTuple(Tuple tuple) {
                tickHandled.set(true);
            }

            @Override
            protected void process(Tuple tuple) {
                tupleProcessed.set(true);
            }
        };

        // Tick tuple mock
        Tuple tickTuple = Mockito.mock(Tuple.class);
        Mockito.when(tickTuple.getSourceComponent()).thenReturn(Constants.SYSTEM_COMPONENT_ID);
        Mockito.when(tickTuple.getSourceStreamId()).thenReturn(Constants.SYSTEM_TICK_STREAM_ID);

        bolt.execute(tickTuple);
        Assert.assertTrue(tickHandled.get());
        Assert.assertFalse(tupleProcessed.get());

        // Regular tuple mock
        tickHandled.set(false);
        Tuple regularTuple = Mockito.mock(Tuple.class);
        Mockito.when(regularTuple.getSourceComponent()).thenReturn("custom-spout");
        Mockito.when(regularTuple.getSourceStreamId()).thenReturn("default");

        bolt.execute(regularTuple);
        Assert.assertFalse(tickHandled.get());
        Assert.assertTrue(tupleProcessed.get());
    }
}

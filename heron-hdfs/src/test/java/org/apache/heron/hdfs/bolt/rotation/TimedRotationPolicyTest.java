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

package org.apache.heron.hdfs.bolt.rotation;

import org.apache.heron.api.tuple.Tuple;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TimedRotationPolicyTest {

    @Test
    public void testTimedRotationPolicyInterval() {
        TimedRotationPolicy policy = new TimedRotationPolicy(5.0f, TimedRotationPolicy.TimeUnit.MINUTES);
        Assert.assertEquals(policy.getInterval(), 5 * 60 * 1000L);

        Tuple tuple = Mockito.mock(Tuple.class);
        Assert.assertFalse(policy.mark(tuple, 100L)); // Always false since timer handles rotation

        FileRotationPolicy copy = policy.copy();
        Assert.assertNotNull(copy);
        Assert.assertEquals(((TimedRotationPolicy) copy).getInterval(), 5 * 60 * 1000L);
    }
}

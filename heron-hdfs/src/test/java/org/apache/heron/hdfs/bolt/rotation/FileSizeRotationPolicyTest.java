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

public class FileSizeRotationPolicyTest {

    @Test
    public void testFileSizeRotationPolicyTrigger() {
        FileSizeRotationPolicy policy = new FileSizeRotationPolicy(1.0f, FileSizeRotationPolicy.Units.KB);
        Tuple tuple = Mockito.mock(Tuple.class);

        Assert.assertFalse(policy.mark(tuple, 500L));
        Assert.assertFalse(policy.mark(tuple, 1000L));
        Assert.assertTrue(policy.mark(tuple, 1024L)); // Reached 1KB

        policy.reset();
        Assert.assertFalse(policy.mark(tuple, 100L));
    }

    @Test
    public void testFileSizeRotationPolicyCopy() {
        FileSizeRotationPolicy policy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);
        FileRotationPolicy copy = policy.copy();
        Assert.assertNotNull(copy);
        Assert.assertTrue(copy instanceof FileSizeRotationPolicy);
    }
}

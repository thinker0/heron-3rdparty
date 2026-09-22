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

import org.apache.heron.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KafkaSpoutRetryExponentialBackoffTest {

    @Test
    public void testExponentialBackoffScheduling() {
        KafkaSpoutRetryExponentialBackoff retryService = new KafkaSpoutRetryExponentialBackoff(
            TimeInterval.milliSeconds(10),
            TimeInterval.milliSeconds(20),
            10,
            TimeInterval.seconds(5)
        );

        TopicPartition tp = new TopicPartition("test-topic", 0);
        KafkaSpoutMessageId msgId = new KafkaSpoutMessageId(tp, 100L);

        Assert.assertFalse(retryService.isScheduled(msgId));

        // Schedule first fail
        msgId.incrementNumFails();
        boolean scheduled = retryService.schedule(msgId);
        Assert.assertTrue(scheduled);
        Assert.assertTrue(retryService.isScheduled(msgId));
        Assert.assertEquals(retryService.getMessageId(tp, 100L).numFails(), 1);

        // Schedule second fail
        msgId.incrementNumFails();
        scheduled = retryService.schedule(msgId);
        Assert.assertTrue(scheduled);
        Assert.assertEquals(retryService.getMessageId(tp, 100L).numFails(), 2);

        // Remove
        boolean removed = retryService.remove(msgId);
        Assert.assertTrue(removed);
        Assert.assertFalse(retryService.isScheduled(msgId));
    }

    @Test
    public void testLargeNumFailsDoesNotOverflowOrSpin() {
        KafkaSpoutRetryExponentialBackoff retryService = new KafkaSpoutRetryExponentialBackoff(
            TimeInterval.milliSeconds(10),
            TimeInterval.milliSeconds(20),
            100,
            TimeInterval.seconds(60)
        );

        TopicPartition tp = new TopicPartition("overflow-topic", 1);
        KafkaSpoutMessageId msgId = new KafkaSpoutMessageId(tp, 42L);

        // Simulate 70 consecutive failures (which would cause 2^69 long overflow in unhardened code)
        for (int i = 1; i <= 70; i++) {
            msgId.incrementNumFails();
            boolean scheduled = retryService.schedule(msgId);
            Assert.assertTrue(scheduled);
        }

        Assert.assertTrue(retryService.isScheduled(msgId));
        Assert.assertEquals(retryService.getMessageId(tp, 42L).numFails(), 70);
    }

    @Test
    public void testExceedingMaxRetriesReturnsFalse() {
        KafkaSpoutRetryExponentialBackoff retryService = new KafkaSpoutRetryExponentialBackoff(
            TimeInterval.milliSeconds(10),
            TimeInterval.milliSeconds(20),
            3,
            TimeInterval.seconds(5)
        );

        TopicPartition tp = new TopicPartition("retry-limit-topic", 0);
        KafkaSpoutMessageId msgId = new KafkaSpoutMessageId(tp, 1L);

        msgId.incrementNumFails();
        Assert.assertTrue(retryService.schedule(msgId)); // fail 1
        msgId.incrementNumFails();
        Assert.assertTrue(retryService.schedule(msgId)); // fail 2
        msgId.incrementNumFails();
        Assert.assertTrue(retryService.schedule(msgId)); // fail 3
        msgId.incrementNumFails();
        Assert.assertFalse(retryService.schedule(msgId)); // fail 4 -> exceeds maxRetries=3
        Assert.assertFalse(retryService.isScheduled(msgId));
    }
}

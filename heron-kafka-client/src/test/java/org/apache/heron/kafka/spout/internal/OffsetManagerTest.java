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

package org.apache.heron.kafka.spout.internal;

import org.apache.heron.kafka.spout.KafkaSpoutMessageId;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OffsetManagerTest {

    @Test
    public void testSequentialAckAndCommit() {
        TopicPartition tp = new TopicPartition("test-topic", 0);
        OffsetManager offsetManager = new OffsetManager(tp, 0L);

        // Emit 0, 1, 2
        offsetManager.addToEmitMsgs(0L);
        offsetManager.addToEmitMsgs(1L);
        offsetManager.addToEmitMsgs(2L);

        Assert.assertEquals(offsetManager.getNumUncommittedOffsets(), 3);
        Assert.assertEquals(offsetManager.getLatestEmittedOffset(), 2L);
        Assert.assertNull(offsetManager.findNextCommitOffset("meta"));

        // Ack offset 0
        KafkaSpoutMessageId msg0 = new KafkaSpoutMessageId(tp, 0L);
        offsetManager.addToAckMsgs(msg0);

        OffsetAndMetadata commitOffset = offsetManager.findNextCommitOffset("meta");
        Assert.assertNotNull(commitOffset);
        Assert.assertEquals(commitOffset.offset(), 1L);

        // Ack offset 1 & 2
        offsetManager.addToAckMsgs(new KafkaSpoutMessageId(tp, 1L));
        offsetManager.addToAckMsgs(new KafkaSpoutMessageId(tp, 2L));

        commitOffset = offsetManager.findNextCommitOffset("meta");
        Assert.assertNotNull(commitOffset);
        Assert.assertEquals(commitOffset.offset(), 3L);

        // Commit
        long committedCount = offsetManager.commit(commitOffset);
        Assert.assertEquals(committedCount, 3L);
        Assert.assertEquals(offsetManager.getCommittedOffset(), 3L);
        Assert.assertEquals(offsetManager.getNumUncommittedOffsets(), 0);
        Assert.assertTrue(offsetManager.hasCommitted());
    }

    @Test
    public void testOutOfOrderAckDoesNotCommitGaps() {
        TopicPartition tp = new TopicPartition("test-topic", 0);
        OffsetManager offsetManager = new OffsetManager(tp, 0L);

        offsetManager.addToEmitMsgs(0L);
        offsetManager.addToEmitMsgs(1L);
        offsetManager.addToEmitMsgs(2L);

        // Ack offset 1 without acking offset 0
        offsetManager.addToAckMsgs(new KafkaSpoutMessageId(tp, 1L));
        Assert.assertNull(offsetManager.findNextCommitOffset("meta"));

        // Now ack offset 0
        offsetManager.addToAckMsgs(new KafkaSpoutMessageId(tp, 0L));
        OffsetAndMetadata commitOffset = offsetManager.findNextCommitOffset("meta");
        Assert.assertNotNull(commitOffset);
        Assert.assertEquals(commitOffset.offset(), 2L); // both 0 and 1 are ready
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class)
    public void testGetNthUncommittedOffsetZeroIndexThrows() {
        TopicPartition tp = new TopicPartition("test-topic", 0);
        OffsetManager offsetManager = new OffsetManager(tp, 0L);
        offsetManager.addToEmitMsgs(10L);
        offsetManager.getNthUncommittedOffsetAfterCommittedOffset(0);
    }

    @Test(expectedExceptions = IndexOutOfBoundsException.class)
    public void testGetNthUncommittedOffsetExceedsSizeThrows() {
        TopicPartition tp = new TopicPartition("test-topic", 0);
        OffsetManager offsetManager = new OffsetManager(tp, 0L);
        offsetManager.addToEmitMsgs(10L);
        offsetManager.getNthUncommittedOffsetAfterCommittedOffset(2);
    }
}

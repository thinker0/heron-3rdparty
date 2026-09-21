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

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionType;
import org.testng.annotations.Test;

public class PulsarConfigurationTest {

    @Test
    public void testPulsarHeronConfiguration() {
        PulsarHeronConfiguration conf = new PulsarHeronConfiguration();

        assertEquals(conf.getMetricsTimeIntervalInSecs(), 60);

        conf.setServiceUrl("pulsar://localhost:6650");
        assertEquals(conf.getServiceUrl(), "pulsar://localhost:6650");

        conf.setTopic("persistent://sample/ns1/t1");
        assertEquals(conf.getTopic(), "persistent://sample/ns1/t1");
        assertEquals(conf.getTopicNameOrPattern(), "persistent://sample/ns1/t1");

        Set<String> topics = new HashSet<>();
        topics.add("t1");
        topics.add("t2");
        conf.setTopicNames(topics);
        assertEquals(conf.getTopicNames(), topics);
        assertTrue(conf.getTopicNameOrPattern().contains("t1") && conf.getTopicNameOrPattern().contains("t2"));

        conf.setTopicNames(null);
        Pattern pattern = Pattern.compile("persistent://sample/ns1/.*");
        conf.setTopicPattern(pattern);
        assertEquals(conf.getTopicPattern(), pattern);
        assertEquals(conf.getTopicNameOrPattern(), "persistent://sample/ns1/.*");

        conf.setMetricsTimeIntervalInSecs(30);
        assertEquals(conf.getMetricsTimeIntervalInSecs(), 30);

        expectThrows(IllegalArgumentException.class, () -> conf.setMetricsTimeIntervalInSecs(0));
        expectThrows(IllegalArgumentException.class, () -> conf.setMetricsTimeIntervalInSecs(-5));
    }

    @Test
    public void testPulsarBoltConfiguration() {
        PulsarBoltConfiguration conf = new PulsarBoltConfiguration();
        assertNull(conf.getTupleToMessageMapper());

        TupleToMessageMapper mapper = mock(TupleToMessageMapper.class);
        conf.setTupleToMessageMapper(mapper);
        assertEquals(conf.getTupleToMessageMapper(), mapper);

        expectThrows(NullPointerException.class, () -> conf.setTupleToMessageMapper(null));
    }

    @Test
    public void testPulsarSpoutConfiguration() {
        PulsarSpoutConfiguration conf = new PulsarSpoutConfiguration();

        // Check default values
        assertEquals(conf.getSubscriptionType(), SubscriptionType.Shared);
        assertEquals(conf.getMaxFailedRetries(), -1);
        assertEquals(conf.getFailedRetriesTimeout(TimeUnit.SECONDS), 60);
        assertFalse(conf.isSharedConsumerEnabled());
        assertFalse(conf.isAutoUnsubscribe());
        assertTrue(conf.isDurableSubscription());
        assertEquals(conf.getNonDurableSubscriptionReadPosition(), MessageId.earliest);

        conf.setSubscriptionName("sub-test");
        assertEquals(conf.getSubscriptionName(), "sub-test");

        conf.setSubscriptionType(SubscriptionType.Failover);
        assertEquals(conf.getSubscriptionType(), SubscriptionType.Failover);

        MessageToValuesMapper mapper = mock(MessageToValuesMapper.class);
        conf.setMessageToValuesMapper(mapper);
        assertEquals(conf.getMessageToValuesMapper(), mapper);
        expectThrows(NullPointerException.class, () -> conf.setMessageToValuesMapper(null));

        conf.setFailedRetriesTimeout(120, TimeUnit.SECONDS);
        assertEquals(conf.getFailedRetriesTimeout(TimeUnit.SECONDS), 120);

        conf.setMaxFailedRetries(5);
        assertEquals(conf.getMaxFailedRetries(), 5);

        conf.setSharedConsumerEnabled(true);
        assertTrue(conf.isSharedConsumerEnabled());

        conf.setAutoUnsubscribe(true);
        assertTrue(conf.isAutoUnsubscribe());

        conf.setDurableSubscription(false);
        assertFalse(conf.isDurableSubscription());

        conf.setNonDurableSubscriptionReadPosition(MessageId.latest);
        assertEquals(conf.getNonDurableSubscriptionReadPosition(), MessageId.latest);
    }
}

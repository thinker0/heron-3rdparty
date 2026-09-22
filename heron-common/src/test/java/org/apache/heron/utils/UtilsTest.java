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

package org.apache.heron.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.heron.api.Config;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UtilsTest {

    @Test
    public void testPutTickFrequencyIntoComponentConfigConversion() {
        Map<String, Object> conf = new HashMap<>();
        Map<String, Object> result = Utils.putTickFrequencyIntoComponentConfig(conf, 15);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.containsKey(Config.TOPOLOGY_TICK_TUPLE_FREQ_MS));
        Assert.assertEquals(result.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_MS), 15000L);
    }

    @Test
    public void testPutTickFrequencyIntoComponentConfigNullMap() {
        Map<String, Object> result = Utils.putTickFrequencyIntoComponentConfig(null, 10);

        Assert.assertNotNull(result);
        Assert.assertEquals(result.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_MS), 10000L);
    }

    @Test
    public void testPutTickFrequencyIntoComponentConfigZeroOrNegative() {
        Map<String, Object> conf = new HashMap<>();
        Map<String, Object> result = Utils.putTickFrequencyIntoComponentConfig(conf, 0);
        Assert.assertFalse(result.containsKey(Config.TOPOLOGY_TICK_TUPLE_FREQ_MS));

        result = Utils.putTickFrequencyIntoComponentConfig(conf, -5);
        Assert.assertFalse(result.containsKey(Config.TOPOLOGY_TICK_TUPLE_FREQ_MS));
    }

    @Test
    public void testParseJvmHeapMemByChildOptsGigabytes() {
        Assert.assertEquals(Utils.parseJvmHeapMemByChildOpts(Collections.singletonList("-Xmx1g"), 100.0), 1024.0);
        Assert.assertEquals(Utils.parseJvmHeapMemByChildOpts(Collections.singletonList("-Xmx2g"), 100.0), 2048.0);
        Assert.assertEquals(Utils.parseJvmHeapMemByChildOpts(Collections.singletonList("-Xmx4g"), 100.0), 4096.0);
        Assert.assertEquals(Utils.parseJvmHeapMemByChildOpts(Collections.singletonList("-Xmx16G"), 100.0), 16384.0);
        Assert.assertEquals(Utils.parseJvmHeapMemByChildOpts(Collections.singletonList("-Xmx32g"), 100.0), 32768.0);
    }

    @Test
    public void testParseJvmHeapMemByChildOptsMegabytesAndKilobytes() {
        Assert.assertEquals(Utils.parseJvmHeapMemByChildOpts(Collections.singletonList("-Xmx512m"), 100.0), 512.0);
        Assert.assertEquals(Utils.parseJvmHeapMemByChildOpts(Collections.singletonList("-Xmx1024M"), 100.0), 1024.0);
        Assert.assertEquals(Utils.parseJvmHeapMemByChildOpts(Collections.singletonList("-Xmx1048576k"), 100.0), 1024.0);
        Assert.assertEquals(Utils.parseJvmHeapMemByChildOpts(null, 256.0), 256.0);
        Assert.assertEquals(Utils.parseJvmHeapMemByChildOpts(Collections.singletonList("-Dsome.prop=value"), 256.0), 256.0);
    }

    @Test
    public void testRedactValue() {
        Map<String, Object> map = new HashMap<>();
        map.put("password", "secret123");
        map.put("user", "admin");

        Map<String, Object> redacted = Utils.redactValue(map, "password");
        Assert.assertEquals(redacted.get("password"), "#########");
        Assert.assertEquals(redacted.get("user"), "admin");
        Assert.assertEquals(map.get("password"), "secret123"); // Original map unmodified

        // Null value for key
        Map<String, Object> mapWithNull = new HashMap<>();
        mapWithNull.put("secret", null);
        Map<String, Object> redactedNull = Utils.redactValue(mapWithNull, "secret");
        Assert.assertNull(redactedNull.get("secret"));

        // Non-existent key
        Map<String, Object> unchanged = Utils.redactValue(map, "nonexistent");
        Assert.assertEquals(unchanged, map);

        // Null map
        Assert.assertNull(Utils.redactValue(null, "key"));
    }

    @Test
    public void testConvertToArray() {
        Map<Integer, String> map = new HashMap<>();
        map.put(2, "B");
        map.put(3, "C");
        map.put(5, "E");

        ArrayList<String> array = Utils.convertToArray(map, 2);
        Assert.assertEquals(array.size(), 4); // indices 0 (id 2), 1 (id 3), 2 (id 4 - null), 3 (id 5)
        Assert.assertEquals(array.get(0), "B");
        Assert.assertEquals(array.get(1), "C");
        Assert.assertNull(array.get(2));
        Assert.assertEquals(array.get(3), "E");

        // Empty and null maps
        Assert.assertTrue(Utils.convertToArray(new HashMap<Integer, String>(), 0).isEmpty());
        Assert.assertTrue(Utils.convertToArray(null, 0).isEmpty());

        // start greater than largestId
        Assert.assertTrue(Utils.convertToArray(map, 10).isEmpty());
    }

    @Test
    public void testPartitionFixed() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        List<List<Integer>> chunks = Utils.partitionFixed(3, list);
        Assert.assertEquals(chunks.size(), 3);
        int totalElements = chunks.stream().mapToInt(List::size).sum();
        Assert.assertEquals(totalElements, 10);

        // Edge cases
        Assert.assertTrue(Utils.partitionFixed(0, list).isEmpty());
        Assert.assertTrue(Utils.partitionFixed(-1, list).isEmpty());
        Assert.assertTrue(Utils.partitionFixed(3, null).isEmpty());
        Assert.assertTrue(Utils.partitionFixed(3, Collections.emptyList()).isEmpty());
    }

    @Test
    public void testIntegerDivided() {
        TreeMap<Integer, Integer> result = Utils.integerDivided(10, 3);
        Assert.assertNotNull(result);
        Assert.assertEquals(result.get(3), Integer.valueOf(2)); // base 3 appears 2 times
        Assert.assertEquals(result.get(4), Integer.valueOf(1)); // base 4 appears 1 time (3*2 + 4*1 = 10)
    }
}

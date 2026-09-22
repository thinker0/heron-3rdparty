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

package org.apache.heron.hdfs.avro;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FixedAvroSerializerTest {

    private static final String SCHEMA_USER = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";
    private static final String SCHEMA_EVENT = "{\"type\":\"record\",\"name\":\"Event\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"payload\",\"type\":\"string\"}]}";

    @Test
    public void testFixedAvroSerializerFromInputStream() throws Exception {
        String configContent = "# Comment line\n" + SCHEMA_USER + "\n\n" + SCHEMA_EVENT + "\n";
        ByteArrayInputStream in = new ByteArrayInputStream(configContent.getBytes(StandardCharsets.UTF_8));

        FixedAvroSerializer serializer = new FixedAvroSerializer(in);

        Schema userSchema = new Schema.Parser().parse(SCHEMA_USER);
        Schema eventSchema = new Schema.Parser().parse(SCHEMA_EVENT);

        String userFp = serializer.getFingerprint(userSchema);
        String eventFp = serializer.getFingerprint(eventSchema);

        Assert.assertNotNull(userFp);
        Assert.assertNotNull(eventFp);
        Assert.assertNotEquals(userFp, eventFp);

        // Verify reverse mapping
        Schema retrievedUserSchema = serializer.getSchema(userFp);
        Schema retrievedEventSchema = serializer.getSchema(eventFp);

        Assert.assertEquals(retrievedUserSchema, userSchema);
        Assert.assertEquals(retrievedEventSchema, eventSchema);
    }

    @Test(expectedExceptions = IOException.class)
    public void testFixedAvroSerializerNullStreamThrows() throws Exception {
        new FixedAvroSerializer((ByteArrayInputStream) null);
    }
}

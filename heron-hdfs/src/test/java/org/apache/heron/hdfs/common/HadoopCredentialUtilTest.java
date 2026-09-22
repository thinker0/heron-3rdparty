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

package org.apache.heron.hdfs.common;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HadoopCredentialUtilTest {

    @Test
    public void testGetCredentialWithValidSerializedCredentials() throws Exception {
        Credentials originalCreds = new Credentials();
        Text alias = new Text("hdfs://test-cluster:8020");
        Token<?> token = new Token<>("identifier".getBytes(), "password".getBytes(), new Text("kind"), new Text("service"));
        originalCreds.addToken(alias, token);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            originalCreds.writeTokenStorageToStream(dos);
        }
        String base64Creds = Base64.getEncoder().encodeToString(baos.toByteArray());

        CredentialKeyProvider provider = configKey -> "HDFS_CREDENTIALS";
        Map<String, String> credsMap = new HashMap<>();
        credsMap.put("HDFS_CREDENTIALS", base64Creds);

        Set<Pair<String, Credentials>> result = HadoopCredentialUtil.getCredential(provider, credsMap, Collections.singletonList("hdfs-config"));
        Assert.assertNotNull(result);
        Assert.assertEquals(result.size(), 1);

        Pair<String, Credentials> pair = result.iterator().next();
        Assert.assertEquals(pair.getFirst(), "hdfs-config");
        Credentials recovered = pair.getSecond();
        Assert.assertNotNull(recovered);
        Assert.assertNotNull(recovered.getToken(alias));
        Assert.assertEquals(recovered.getToken(alias).getService(), new Text("service"));
    }

    @Test
    public void testGetCredentialWithWritableWriteFormat() throws Exception {
        Credentials originalCreds = new Credentials();
        Text alias = new Text("hdfs://test-cluster:8020");
        Token<?> token = new Token<>("identifier2".getBytes(), "password2".getBytes(), new Text("kind2"), new Text("service2"));
        originalCreds.addToken(alias, token);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            originalCreds.write(dos);
        }
        String base64Creds = Base64.getEncoder().encodeToString(baos.toByteArray());

        CredentialKeyProvider provider = configKey -> "HDFS_CREDENTIALS";
        Map<String, String> credsMap = Collections.singletonMap("HDFS_CREDENTIALS", base64Creds);

        Set<Pair<String, Credentials>> result = HadoopCredentialUtil.getCredential(provider, credsMap, Collections.singletonList("hdfs-config"));
        Assert.assertNotNull(result);
        Assert.assertEquals(result.size(), 1);

        Pair<String, Credentials> pair = result.iterator().next();
        Assert.assertEquals(pair.getFirst(), "hdfs-config");
        Credentials recovered = pair.getSecond();
        Assert.assertNotNull(recovered);
        Assert.assertNotNull(recovered.getToken(alias));
        Assert.assertEquals(recovered.getToken(alias).getService(), new Text("service2"));
    }

    @Test
    public void testGetCredentialWithEmptyConfigKeys() throws Exception {
        Credentials originalCreds = new Credentials();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            originalCreds.writeTokenStorageToStream(dos);
        }
        String base64Creds = Base64.getEncoder().encodeToString(baos.toByteArray());

        CredentialKeyProvider provider = configKey -> "HDFS_CREDENTIALS";
        Map<String, String> credsMap = Collections.singletonMap("HDFS_CREDENTIALS", base64Creds);

        Set<Pair<String, Credentials>> result = HadoopCredentialUtil.getCredential(provider, credsMap, Collections.emptyList());
        Assert.assertNotNull(result);
        Assert.assertEquals(result.size(), 1);
        Pair<String, Credentials> pair = result.iterator().next();
        Assert.assertEquals(pair.getFirst(), "");
        Assert.assertNotNull(pair.getSecond());
    }

    @Test
    public void testGetCredentialWithMissingOrInvalidKey() {
        CredentialKeyProvider provider = configKey -> "HDFS_CREDENTIALS";
        Map<String, String> credsMap = new HashMap<>();
        credsMap.put("HDFS_CREDENTIALS", "invalid-not-base64-!@#$");

        Set<Pair<String, Credentials>> result = HadoopCredentialUtil.getCredential(provider, credsMap, Collections.singletonList("key1"));
        Assert.assertNotNull(result);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testGetCredentialWithNullMap() {
        CredentialKeyProvider provider = configKey -> "HDFS_CREDENTIALS";
        Set<Pair<String, Credentials>> result = HadoopCredentialUtil.getCredential(provider, null, Collections.singletonList("key1"));
        Assert.assertNotNull(result);
        Assert.assertTrue(result.isEmpty());
    }
}

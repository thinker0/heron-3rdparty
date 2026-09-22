/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.heron.hdfs.avro;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;

/**
 * A class to help (de)serialize a pre-defined set of Avro schemas.  Schemas should be listed, one per line, in a file
 * called "FixedAvroSerializer.config", which must be part of the Storm topology jar file.  Any schemas intended to be
 * used with this class **MUST** be defined in that file.
 */
public class FixedAvroSerializer extends AbstractAvroSerializer {

    private static final String FP_ALGO = "CRC-64-AVRO";
    private static final String DEFAULT_CONFIG_FILE = "FixedAvroSerializer.config";
    final Map<String, Schema> fingerprint2schemaMap = new HashMap<>();
    final Map<Schema, String> schema2fingerprintMap = new HashMap<>();

    public FixedAvroSerializer() throws IOException, NoSuchAlgorithmException {
        this(DEFAULT_CONFIG_FILE);
    }

    public FixedAvroSerializer(String configResource) throws IOException, NoSuchAlgorithmException {
        this(loadConfigStream(configResource));
    }

    public FixedAvroSerializer(InputStream in) throws IOException, NoSuchAlgorithmException {
        if (in == null) {
            throw new IOException("Input stream for Avro schemas configuration is null");
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                Schema schema = new Schema.Parser().parse(line);
                byte[] fp = SchemaNormalization.parsingFingerprint(FP_ALGO, schema);
                String fingerPrint = Base64.getEncoder().encodeToString(fp);

                fingerprint2schemaMap.put(fingerPrint, schema);
                schema2fingerprintMap.put(schema, fingerPrint);
            }
        }
    }

    private static InputStream loadConfigStream(String configResource) throws IOException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        InputStream in = cl != null ? cl.getResourceAsStream(configResource) : null;
        if (in == null) {
            in = FixedAvroSerializer.class.getClassLoader().getResourceAsStream(configResource);
        }
        if (in == null) {
            throw new IOException("Avro configuration file not found in classpath: " + configResource);
        }
        return in;
    }

    @Override
    public String getFingerprint(Schema schema) {
        return schema2fingerprintMap.get(schema);
    }

    @Override
    public Schema getSchema(String fingerPrint) {
        return fingerprint2schemaMap.get(fingerPrint);
    }
}

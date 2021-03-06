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

package org.apache.heron.hdfs.security;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.heron.hdfs.security.auth.kerbros.AutoTGT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides util methods for storm-hdfs connector communicating
 * with secured HDFS.
 */
public final class HdfsSecurityUtil {
    /**
     * A list of IAutoCredentials that the topology should load and use.
     */
    public static final String TOPOLOGY_AUTO_CREDENTIALS = "topology.auto-credentials";
    public static final String HERON_KEYTAB_FILE_KEY = "hdfs.keytab.file";
    public static final String HERON_USER_NAME_KEY = "hdfs.kerberos.principal";
    public static final String HDFS_CREDENTIALS_CONFIG_KEYS = "hdfsCredentialsConfigKeys";
    public static final String HDFS_CREDENTIALS = "HDFS_CREDENTIALS";
    public static final String TOPOLOGY_HDFS_URI = "topology.hdfs.uri";

    private static final Logger LOG = LoggerFactory.getLogger(HdfsSecurityUtil.class);
    private static AtomicBoolean isLoggedIn = new AtomicBoolean();

    private HdfsSecurityUtil() {
    }

    public static void login(Map<String, Object> conf, Configuration hdfsConfig) throws IOException {
        //If AutoHDFS is specified, do not attempt to login using keytabs, only kept for backward compatibility.
        if (conf.get(TOPOLOGY_AUTO_CREDENTIALS) == null
                || (!(((List) conf.get(TOPOLOGY_AUTO_CREDENTIALS)).contains(AutoHDFS.class.getName()))
                        && !(((List) conf.get(TOPOLOGY_AUTO_CREDENTIALS)).contains(AutoTGT.class.getName())))) {
            if (UserGroupInformation.isSecurityEnabled()) {
                // compareAndSet added because of https://issues.apache.org/jira/browse/STORM-1535
                if (isLoggedIn.compareAndSet(false, true)) {
                    LOG.info("Logging in using keytab as AutoHDFS is not specified for " + TOPOLOGY_AUTO_CREDENTIALS);
                    String keytab = (String) conf.get(HERON_KEYTAB_FILE_KEY);
                    if (keytab != null) {
                        hdfsConfig.set(HERON_KEYTAB_FILE_KEY, keytab);
                    }
                    String userName = (String) conf.get(HERON_USER_NAME_KEY);
                    if (userName != null) {
                        hdfsConfig.set(HERON_USER_NAME_KEY, userName);
                    }
                    SecurityUtil.login(hdfsConfig, HERON_KEYTAB_FILE_KEY, HERON_USER_NAME_KEY);
                }
            }
        }
    }
}
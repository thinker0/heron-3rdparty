/*
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

package org.apache.heron.hdfs.security.auth;

import static org.apache.heron.hdfs.security.HdfsSecurityUtil.TOPOLOGY_AUTO_CREDENTIALS;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.security.URIParameter;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.apache.heron.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientAuthUtils {
    public static final String LOGIN_CONTEXT_SERVER = "StormServer";
    public static final String LOGIN_CONTEXT_CLIENT = "StormClient";
    public static final String LOGIN_CONTEXT_PACEMAKER_DIGEST = "PacemakerDigest";
    public static final String LOGIN_CONTEXT_PACEMAKER_SERVER = "PacemakerServer";
    public static final String LOGIN_CONTEXT_PACEMAKER_CLIENT = "PacemakerClient";
    public static final String SERVICE = "storm_thrift_server";
    private static final Logger LOG = LoggerFactory.getLogger(ClientAuthUtils.class);
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    public static String getJaasConf(Map<String, Object> topoConf) {
        return (String) topoConf.get("java.security.auth.login.config");
    }

    /**
     * Construct a JAAS configuration object per storm configuration file.
     *
     * @param topoConf Storm configuration
     * @return JAAS configuration object
     */
    public static Configuration getConfiguration(Map<String, Object> topoConf) {
        Configuration loginConf = null;

        //find login file configuration from Storm configuration
        String loginConfigurationFile = getJaasConf(topoConf);
        if ((loginConfigurationFile != null) && (loginConfigurationFile.length() > 0)) {
            File configFile = new File(loginConfigurationFile);
            if (!configFile.canRead()) {
                throw new RuntimeException("File " + loginConfigurationFile + " cannot be read.");
            }
            try {
                URI configUri = configFile.toURI();
                loginConf = Configuration.getInstance("JavaLoginConfig", new URIParameter(configUri));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        return loginConf;
    }

    /**
     * Get configurations for a section.
     *
     * @param configuration The config to pull the key/value pairs out of.
     * @param section       The app configuration entry name to get stuff from.
     * @return Return array of config entries or null if configuration is null
     */
    public static AppConfigurationEntry[] getEntries(Configuration configuration,
                                                     String section) throws IOException {
        if (configuration == null) {
            return null;
        }

        AppConfigurationEntry[] configurationEntries = configuration.getAppConfigurationEntry(section);
        if (configurationEntries == null) {
            String errorMessage = "Could not find a '" + section + "' entry in this configuration.";
            throw new IOException(errorMessage);
        }
        return configurationEntries;
    }

    /**
     * Pull a set of keys out of a Configuration.
     *
     * @param topoConf  The config containing the jaas conf file.
     * @param section       The app configuration entry name to get stuff from.
     * @return Return a map of the configs in conf.
     */
    public static SortedMap<String, ?> pullConfig(Map<String, Object> topoConf,
                                                  String section) throws IOException {
        Configuration configuration = ClientAuthUtils.getConfiguration(topoConf);
        AppConfigurationEntry[] configurationEntries = ClientAuthUtils.getEntries(configuration, section);

        if (configurationEntries == null) {
            return null;
        }

        TreeMap<String, Object> results = new TreeMap<>();

        for (AppConfigurationEntry entry : configurationEntries) {
            Map<String, ?> options = entry.getOptions();
            for (String key : options.keySet()) {
                results.put(key, options.get(key));
            }
        }

        return results;
    }

    /**
     * Pull a the value given section and key from Configuration.
     *
     * @param topoConf   The config containing the jaas conf file.
     * @param section       The app configuration entry name to get stuff from.
     * @param key           The key to look up inside of the section
     * @return Return a the String value of the configuration value
     */
    public static String get(
        Map<String, Object> topoConf, String section, String key) throws IOException {
        Configuration configuration = ClientAuthUtils.getConfiguration(topoConf);
        return get(configuration, section, key);
    }

    static String get(Configuration configuration, String section, String key) throws IOException {
        AppConfigurationEntry[] configurationEntries = ClientAuthUtils.getEntries(configuration, section);

        if (configurationEntries == null) {
            return null;
        }

        for (AppConfigurationEntry entry : configurationEntries) {
            Object val = entry.getOptions().get(key);
            if (val != null) {
                return (String) val;
            }
        }
        return null;
    }

    /**
     * Get all of the configured AutoCredential Plugins.
     *
     * @param topoConf the storm configuration to use.
     * @return the configured auto credentials.
     */
    public static Collection<IAutoCredentials> getAutoCredentials(Map<String, Object> topoConf) {
        try {
            Set<IAutoCredentials> autos = new HashSet<>();
            Collection<String> clazzes = (Collection<String>) topoConf.get(TOPOLOGY_AUTO_CREDENTIALS);
            if (clazzes != null) {
                for (String clazz : clazzes) {
                    IAutoCredentials a = ReflectionUtils.newInstance(clazz);
                    a.prepare(topoConf);
                    autos.add(a);
                }
            }
            LOG.info("Got AutoCreds " + autos);
            return autos;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] serializeKerberosTicket(KerberosTicket tgt) throws Exception {
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bao);
        out.writeObject(tgt);
        out.flush();
        out.close();
        return bao.toByteArray();
    }

    public static KerberosTicket deserializeKerberosTicket(byte[] tgtBytes) {
        KerberosTicket ret;
        try {

            ByteArrayInputStream bin = new ByteArrayInputStream(tgtBytes);
            ObjectInputStream in = new ObjectInputStream(bin);
            ret = (KerberosTicket) in.readObject();
            in.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    public static KerberosTicket cloneKerberosTicket(KerberosTicket kerberosTicket) {
        if (kerberosTicket != null) {
            try {
                return (deserializeKerberosTicket(serializeKerberosTicket(kerberosTicket)));
            } catch (Exception e) {
                throw new RuntimeException("Failed to clone KerberosTicket TGT!!", e);
            }
        }
        return null;
    }
}

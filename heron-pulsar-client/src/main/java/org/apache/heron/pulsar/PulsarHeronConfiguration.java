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

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import com.beust.jcommander.Strings;

/**
 * Class used to specify pulsar storm configurations like service url and topic
 *
 *
 */
public class PulsarHeronConfiguration implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_METRICS_TIME_INTERVAL_IN_SECS = 60;

    private String serviceUrl = null;
    private String topic = null;
    private Set<String> topicNames = null;
    private Pattern topicPattern = null;
    private int metricsTimeIntervalInSecs = DEFAULT_METRICS_TIME_INTERVAL_IN_SECS;

    /**
     * @return the service URL to connect to from the client
     */
    public String getServiceUrl() {
        return serviceUrl;
    }

    /**
     * Sets the service URL to connect to from the client
     *
     * @param serviceUrl
     */
    public void setServiceUrl(String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    /**
     * @return the topic name for the producer/consumer
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Sets the topic name for the producer/consumer. It should be of the format
     * {persistent|non-persistent}://{property}/{cluster}/{namespace}/{topic}
     *
     * @param topic
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * @return the topic name for the producer/consumer
     */
    public Pattern getTopicPattern() {
        return topicPattern;
    }

    /**
     * Sets the topic name for the producer/consumer. It should be of the format
     * {persistent|non-persistent}://{property}/{cluster}/{namespace}/{topic}
     *
     * @param topicPattern
     */
    public void setTopicPattern(Pattern topicPattern) {
        this.topicPattern = topicPattern;
    }

    /**
     * @return the time interval in seconds for metrics generation
     */
    public int getMetricsTimeIntervalInSecs() {
        return metricsTimeIntervalInSecs;
    }

    /**
     * Sets the time interval in seconds for metrics generation <i>(default: 60 seconds)</i>
     *
     * @param metricsTimeIntervalInSecs
     */
    public void setMetricsTimeIntervalInSecs(int metricsTimeIntervalInSecs) {
        this.metricsTimeIntervalInSecs = metricsTimeIntervalInSecs;
    }

    public Set<String> getTopicNames() {
        return topicNames;
    }

    public void setTopicNames(Set<String> topicNames) {
        this.topicNames = topicNames;
    }

    /**
     * Use only debug
     * @return string
     */
    String getTopicNameOrPattern() {
        final Set<String> topicNames = getTopicNames();
        if (Objects.nonNull(topicNames)) {
            return Strings.join(",", topicNames.toArray());
        }
        final Pattern topicPattern = getTopicPattern();
        if (Objects.nonNull(topicPattern)) {
            return topicPattern.pattern();
        }
        return getTopic();
    }

}

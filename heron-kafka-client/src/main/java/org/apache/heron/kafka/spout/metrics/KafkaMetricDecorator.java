/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.heron.kafka.spout.metrics;

import org.apache.heron.api.metric.IMetric;
import org.apache.kafka.common.Metric;

/**
 * a decorator to convert a Kafka Metric to a Heron Metric so that Kafka
 * metrics can be exposed via Heron Metrics Manager
 *
 * @param <M> the Kafka Metric type
 */
public class KafkaMetricDecorator<M extends Metric> implements IMetric<Object> {
  private M metric;

  public KafkaMetricDecorator(M metric) {
    this.metric = metric;
  }

  @Override
  public Object getValueAndReset() {
    return metric.metricValue();
  }
}

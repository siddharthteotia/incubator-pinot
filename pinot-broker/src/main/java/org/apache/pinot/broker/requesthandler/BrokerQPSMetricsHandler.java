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
package org.apache.pinot.broker.requesthandler;

import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.common.metrics.MetricsHelper;

import static org.apache.pinot.common.utils.CommonConstants.Broker.CONFIG_OF_BROKER_EMIT_MAX_QPS_METRICS;
import static org.apache.pinot.common.utils.CommonConstants.Broker.DEFAULT_BROKER_EMIT_MAX_QPS_METRICS;

/**
 * Handles the setting of QPS related metrics to the metrics registry
 */
public class BrokerQPSMetricsHandler {

  public static final String MAX_QPS_BROKER = "Max-QPS-Broker";

  private final boolean _enabled;
  private final Histogram _inFlightQueriesHistogram;

  BrokerQPSMetricsHandler(final Configuration config, final MetricsRegistry metricsRegistry) {
    _enabled = config.getBoolean(CONFIG_OF_BROKER_EMIT_MAX_QPS_METRICS, DEFAULT_BROKER_EMIT_MAX_QPS_METRICS);
    _inFlightQueriesHistogram = MetricsHelper.newHistogram(metricsRegistry, new MetricName("broker_", "", MAX_QPS_BROKER), false);
  }

  void incrementQueryCount() {
    if (_enabled) {
      _inFlightQueriesHistogram.update(1);
    }
  }

  void decrementQueryCount() {
    if (_enabled) {
      _inFlightQueriesHistogram.update(1);
    }
  }

  long getMaxQPS() {
    return (long)_inFlightQueriesHistogram.max();
  }
}
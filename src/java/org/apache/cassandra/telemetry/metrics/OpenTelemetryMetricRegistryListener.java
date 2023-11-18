/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.telemetry.metrics;

import com.codahale.metrics.*;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.metrics.ObservableLongCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OpenTelemetryMetricRegistryListener implements MetricRegistryListener {

    private static final Logger logger = LoggerFactory.getLogger(OpenTelemetryMetricRegistryListener.class);

    private final io.opentelemetry.api.metrics.Meter otelMeter;

    private final Map<String, AutoCloseable> meters = new ConcurrentHashMap<>();

    public OpenTelemetryMetricRegistryListener(io.opentelemetry.api.metrics.Meter otelMeter)
    {
        logger.info("Adding metric listener");
        this.otelMeter = otelMeter;
    }

    @Override
    public void onGaugeAdded(String s, Gauge<?> gauge)
    {
        logger.info("Adding gauge " + s);
        // OpenTelemetry allows the duplicate name in its registry
        // We skip adding duplicates
        if (meters.containsKey(s)) {
            return;
        }

        ObservableDoubleGauge otelGauge = otelMeter.gaugeBuilder(s).buildWithCallback((m) -> {
            Object obj = gauge.getValue();
            if (obj instanceof Number) {
                m.record(((Number)gauge.getValue()).doubleValue());
            }
        });
        meters.put(s, otelGauge);
    }

    @Override
    public void onGaugeRemoved(String s)
    {
        unregister(s);
    }

    private void unregister(String s)
    {
        logger.info("Removing " + s);
        AutoCloseable metric = meters.remove(s);
        if (metric != null) {
            try {
                metric.close();
            } catch (Exception e) {
                // ignore with WARN
                logger.warn("unregistering metric failed", e);
            }
        }
    }

    @Override
    public void onCounterAdded(String s, Counter counter)
    {
        logger.info("Adding counter " + s);
        ObservableLongCounter otelCounter = otelMeter.counterBuilder(s).buildWithCallback((m) -> {
          m.record(counter.getCount());
        });
    }

    @Override
    public void onCounterRemoved(String s)
    {
        unregister(s);
    }

    /**
     * Histograms are exported as double gauges
     */
    @Override
    public void onHistogramAdded(String s, Histogram histogram)
    {
        logger.info("Adding histogram " + s);

    }

    @Override
    public void onHistogramRemoved(String s)
    {
        unregister(s);
    }

    @Override
    public void onMeterAdded(String s, Meter meter)
    {
        logger.info("Adding meter " + s);

    }

    @Override
    public void onMeterRemoved(String s)
    {
        unregister(s);
    }

    @Override
    public void onTimerAdded(String s, Timer timer)
    {
        logger.info("Adding timer " + s);

    }

    @Override
    public void onTimerRemoved(String s)
    {
        unregister(s);
    }

    /**
     * Instrument naming rule
     * Instrument names MUST conform to the following syntax (described using the Augmented Backus-Naur Form):
     *
     * instrument-name = ALPHA 0*62 ("_" / "." / "-" / ALPHA / DIGIT)
     * ALPHA = %x41-5A / %x61-7A; A-Z / a-z
     * DIGIT = %x30-39 ; 0-9
     *
     * - They are not null or empty strings.
     * - They are case-insensitive, ASCII strings.
     * - The first character must be an alphabetic character.
     * - Subsequent characters must belong to the alphanumeric characters, ‘_’, ‘.’, and ‘-’.
     * - They can have a maximum length of 63 characters.
     */
    private String validateMetricName(String name) {
        return name;
    }
}

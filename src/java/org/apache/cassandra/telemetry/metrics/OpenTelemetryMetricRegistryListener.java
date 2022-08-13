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
import io.opentelemetry.api.metrics.ObservableLongGauge;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Creates OpenTelemetry asynchronous metrics based on Codahale's metrics.
 */
public class OpenTelemetryMetricRegistryListener implements MetricRegistryListener {

    private static final Logger logger = LoggerFactory.getLogger(OpenTelemetryMetricRegistryListener.class);

    private final io.opentelemetry.api.metrics.Meter otelMeter;

    /*
     * OpenTelemetry Java implements acynchronous metrics as AutoClosable, and those are released
     * when closed when no longer necessary.
     * This holds all those asynchronous metrics, and releases when the original metrics are gone.
     */
    private final Map<String, AutoCloseable> meters = new ConcurrentHashMap<>();

    public OpenTelemetryMetricRegistryListener(io.opentelemetry.api.metrics.Meter otelMeter)
    {
        this.otelMeter = otelMeter;
    }

    private void unregister(String s)
    {
        logger.info("Removing " + s);
        AutoCloseable metric = meters.remove(s);
        if (metric != null)
        {
            try
            {
                metric.close();
            }
            catch (Exception e)
            {
                // ignore with WARN
                logger.warn("unregistering metric failed", e);
            }
        }
    }

    @Override
    public void onGaugeAdded(String s, Gauge<?> gauge)
    {
        // OpenTelemetry allows the duplicate name in its registry,
        // so we skip adding duplicates
        if (meters.containsKey(s))
        {
            return;
        }
        MetricParams params = MetricNameTranslator.translate(s);
        if (params == null)
        {
            return;
        }
        logger.info("Adding gauge " + s);
        // Determine the type of Gauge
        Object obj = gauge.getValue();
        if (obj instanceof Long || obj instanceof Integer)
        {
            ObservableLongGauge otelGauge = otelMeter.gaugeBuilder(params.getMetricName()).ofLongs()
                    .buildWithCallback((m) -> m.record(((Number)gauge.getValue()).longValue(), params.getAttributes()));
            meters.put(s, otelGauge);
            logger.info("Added gauge " + params);
        }
        else if (obj instanceof Number)
        {
            ObservableDoubleGauge otelGauge = otelMeter.gaugeBuilder(params.getMetricName())
                    .buildWithCallback((m) -> m.record(((Number)gauge.getValue()).doubleValue(), params.getAttributes()));
            meters.put(s, otelGauge);
            logger.info("Added gauge " + params);
        }
        else if (obj instanceof long[])
        {
            // Estimated partition size and estimated column count are exposed as long[]
            // TODO
            logger.warn("Unsupported gauge(long[]) " + params);
        }
        else
        {
            logger.warn("Unsupported gauge type " + params);
        }
    }

    @Override
    public void onGaugeRemoved(String s)
    {
        unregister(s);
    }

    @Override
    public void onCounterAdded(String s, Counter counter)
    {
        // OpenTelemetry allows the duplicate name in its registry,
        // so we skip adding duplicates
        if (meters.containsKey(s))
        {
            return;
        }
        MetricParams params = MetricNameTranslator.translate(s);
        if (params == null)
        {
            return;
        }
        logger.info("Adding counter " + s);
        ObservableLongCounter otelCounter = otelMeter.counterBuilder(params.getMetricName())
                .buildWithCallback((m) -> m.record(counter.getCount(), params.getAttributes()));
        meters.put(s, otelCounter);
        logger.info("Added counter " + params);
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
        // OpenTelemetry allows the duplicate name in its registry,
        // so we skip adding duplicates
        if (meters.containsKey(s))
        {
            return;
        }
        MetricParams params = MetricNameTranslator.translate(s);
        if (params == null)
        {
            return;
        }
        logger.info("Adding histogram " + s);
        meters.put(s, new HistogramAdapter(histogram, params));
        logger.info("Added histogram " + params);
    }

    @Override
    public void onHistogramRemoved(String s)
    {
        unregister(s);
    }

    @Override
    public void onMeterAdded(String s, Meter meter)
    {
        // OpenTelemetry allows the duplicate name in its registry,
        // so we skip adding duplicates
        if (meters.containsKey(s))
        {
            return;
        }
        MetricParams params = MetricNameTranslator.translate(s);
        if (params == null)
        {
            return;
        }
        logger.info("Adding meter " + s);
        meters.put(s, new MeterAdapter(meter, params));
        logger.info("Added meter " + params);
    }

    @Override
    public void onMeterRemoved(String s)
    {
        unregister(s);
    }

    @Override
    public void onTimerAdded(String s, Timer timer)
    {
        if (meters.containsKey(s))
        {
            return;
        }
        MetricParams params = MetricNameTranslator.translate(s);
        if (params == null)
        {
            return;
        }
        logger.info("Adding timer " + s);
        meters.put(s, new TimerAdapter(timer, params));
        logger.info("Added timer " + params);
    }

    @Override
    public void onTimerRemoved(String s)
    {
        unregister(s);
    }

    private abstract static class Adapter implements AutoCloseable
    {
        protected final Set<AutoCloseable> toClose = new HashSet<>();

        @Override
        public void close() throws Exception
        {
            FBUtilities.closeAll(toClose);
        }
    }
    private class MeterAdapter extends Adapter
    {
        MeterAdapter(Meter meter, MetricParams params)
        {
            toClose.add(otelMeter.counterBuilder(params.getMetricName() + "_count")
                    .buildWithCallback((m) -> m.record(meter.getCount(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_rate_mean")
                    .buildWithCallback((m) -> m.record(meter.getMeanRate(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_rate_m1")
                    .buildWithCallback((m) -> m.record(meter.getOneMinuteRate(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_rate_m5")
                    .buildWithCallback((m) -> m.record(meter.getFiveMinuteRate(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_rate_m15")
                    .buildWithCallback((m) -> m.record(meter.getFifteenMinuteRate(), params.getAttributes())));
        }
    }

    private class HistogramAdapter extends Adapter
    {
        HistogramAdapter(Histogram histogram, MetricParams params)
        {
            Snapshot snapshot = histogram.getSnapshot();
            toClose.add(otelMeter.counterBuilder(params.getMetricName() + "_count")
                    .buildWithCallback((m) -> m.record(histogram.getCount(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_max").ofLongs()
                    .buildWithCallback((m) -> m.record(snapshot.getMax(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_mean")
                    .buildWithCallback((m) -> m.record(snapshot.getMean(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_min").ofLongs()
                    .buildWithCallback((m) -> m.record(snapshot.getMin(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_stddev")
                    .buildWithCallback((m) -> m.record(snapshot.getStdDev(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_p50")
                    .buildWithCallback((m) -> m.record(snapshot.getMedian(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_p75")
                    .buildWithCallback((m) -> m.record(snapshot.get75thPercentile(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_p90")
                    .buildWithCallback((m) -> m.record(snapshot.getValue(0.90), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_p95")
                    .buildWithCallback((m) -> m.record(snapshot.get95thPercentile(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_p98")
                    .buildWithCallback((m) -> m.record(snapshot.get98thPercentile(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_p99")
                    .buildWithCallback((m) -> m.record(snapshot.get99thPercentile(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_p999")
                    .buildWithCallback((m) -> m.record(snapshot.get999thPercentile(), params.getAttributes())));
        }
    }

    private class TimerAdapter extends Adapter
    {
        TimerAdapter(Timer timer, MetricParams params)
        {
            Snapshot snapshot = timer.getSnapshot();

            toClose.add(otelMeter.counterBuilder(params.getMetricName() + "_count")
                    .buildWithCallback((m) -> m.record(timer.getCount(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_rate_mean")
                    .buildWithCallback((m) -> m.record(timer.getMeanRate(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_rate_m1")
                    .buildWithCallback((m) -> m.record(timer.getOneMinuteRate(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_rate_m5")
                    .buildWithCallback((m) -> m.record(timer.getFiveMinuteRate(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_rate_m15")
                    .buildWithCallback((m) -> m.record(timer.getFifteenMinuteRate(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_max").ofLongs()
                    .buildWithCallback((m) -> m.record(snapshot.getMax(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_mean")
                    .buildWithCallback((m) -> m.record(snapshot.getMean(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_min").ofLongs()
                    .buildWithCallback((m) -> m.record(snapshot.getMin(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_stddev")
                    .buildWithCallback((m) -> m.record(snapshot.getStdDev(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_p50")
                    .buildWithCallback((m) -> m.record(snapshot.getMedian(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_p75")
                    .buildWithCallback((m) -> m.record(snapshot.get75thPercentile(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_p90")
                    .buildWithCallback((m) -> m.record(snapshot.getValue(0.90), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_p95")
                    .buildWithCallback((m) -> m.record(snapshot.get95thPercentile(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_p98")
                    .buildWithCallback((m) -> m.record(snapshot.get98thPercentile(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_p99")
                    .buildWithCallback((m) -> m.record(snapshot.get99thPercentile(), params.getAttributes())));
            toClose.add(otelMeter.gaugeBuilder(params.getMetricName() + "_p999")
                    .buildWithCallback((m) -> m.record(snapshot.get999thPercentile(), params.getAttributes())));
        }
    }
}

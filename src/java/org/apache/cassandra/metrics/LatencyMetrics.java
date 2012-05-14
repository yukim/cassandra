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
package org.apache.cassandra.metrics;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

import org.apache.cassandra.utils.EstimatedHistogram;

/**
 * Metrics about latencies
 */
public class LatencyMetrics
{
    /** Number of operation performed */
    public final Counter opCount;
    /** Total latency in micro sec */
    public final Counter totalLatencyMicro;
    public final Gauge<Double> recentLatencyMicro;
    /** Latency histogram */
    public final Gauge<long[]> totalLatencyHistogramMicro;
    /** Latency histogram from last read */
    public final Gauge<long[]> recentLatencyHistogramMicro;

    protected final MetricNameFactory factory;
    protected final String namePrefix;
    protected final EstimatedHistogram totalHistogram;
    protected final EstimatedHistogram recentHistogram;
    protected long lastLatency;
    protected long lastOpCount;

    /**
     * Create LatencyMetrics with given group, type, and scope. Name prefix for each metric will be empty.
     *
     * @param group Group name
     * @param type Type name
     * @param scope Scope
     */
    public LatencyMetrics(String group, String type, String scope)
    {
        this(group, type, "", scope);
    }

    /**
     * Create LatencyMetrics with given group, type, prefix to append to each metric name, and scope.
     *
     * @param group Group name
     * @param type Type name
     * @param namePrefix Prefix to append to each metric name
     * @param scope Scope of metrics
     */
    public LatencyMetrics(String group, String type, String namePrefix, String scope)
    {
        this(new LatencyMetricNameFactory(group, type, scope), namePrefix);
    }

    /**
     * Create LatencyMetrics with given group, type, prefix to append to each metric name, and scope.
     *
     * @param factory MetricName factory to use
     * @param namePrefix Prefix to append to each metric name
     */
    public LatencyMetrics(MetricNameFactory factory, String namePrefix)
    {
        this.factory = factory;
        this.namePrefix = namePrefix;

        opCount = Metrics.newCounter(factory.createMetricName(namePrefix + "Operation"));
        totalLatencyMicro = Metrics.newCounter(factory.createMetricName(namePrefix + "TotalLatencyMicro"));
        recentLatencyMicro = Metrics.newGauge(factory.createMetricName(namePrefix + "RecentLatencyMicro"), new Gauge<Double>()
        {
            public Double value()
            {
                long ops = opCount.count();
                long n = totalLatencyMicro.count();
                try
                {
                    return ((double) n - lastLatency) / (ops - lastOpCount);
                }
                finally
                {
                    lastLatency = n;
                    lastOpCount = ops;
                }
            }
        });
        totalHistogram = new EstimatedHistogram();
        recentHistogram = new EstimatedHistogram();
        totalLatencyHistogramMicro = Metrics.newGauge(factory.createMetricName(namePrefix + "TotalLatencyHistogramMicro"), new Gauge<long[]>()
        {
            public long[] value()
            {
                return totalHistogram.getBuckets(false);
            }
        });
        recentLatencyHistogramMicro = Metrics.newGauge(factory.createMetricName(namePrefix + "RecentLatencyHistogramMicro"), new Gauge<long[]>()
        {
            public long[] value()
            {
                return recentHistogram.getBuckets(true);
            }
        });
    }

    /** takes nanoseconds **/
    public void addNano(long nanos)
    {
        // convert to microseconds. 1 millionth
        addMicro(nanos / 1000);
    }

    public void addMicro(long micros)
    {
        opCount.inc();
        totalLatencyMicro.inc(micros);
        totalHistogram.add(micros);
        recentHistogram.add(micros);
    }

    public void release()
    {
        Metrics.defaultRegistry().removeMetric(factory.createMetricName(namePrefix + "Operation"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName(namePrefix + "TotalLatencyMicro"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName(namePrefix + "RecentLatencyMicro"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName(namePrefix + "TotalLatencyHistogramMicro"));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName(namePrefix + "RecentLatencyHistogramMicro"));
    }

    static class LatencyMetricNameFactory implements MetricNameFactory
    {
        private final String group;
        private final String type;
        private final String scope;

        LatencyMetricNameFactory(String group, String type, String scope)
        {
            this.group = group;
            this.type = type;
            this.scope = scope;
        }

        public MetricName createMetricName(String metricName)
        {
            return new MetricName(group, type, metricName, scope);
        }
    }
}

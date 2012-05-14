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

import java.util.SortedMap;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;

/**
 * Metrics related to Storage.
 */
public class StorageMetrics
{
    public final Gauge<Double> load;

    public StorageMetrics()
    {
        this.load = Metrics.newGauge(new MetricName("org.apache.cassandra.metrics", "Storage", "Load"), new Gauge<Double>()
        {
            public Double value()
            {
                double bytes = 0;
                // find all column family LiveDiscSpaceUsed metrics
                SortedMap<String, SortedMap<MetricName, Metric>> columnFamilyMetrics = Metrics.defaultRegistry().groupedMetrics(new MetricPredicate()
                {
                    public boolean matches(MetricName metricName, Metric metric)
                    {
                        return "org.apache.cassandra.metrics.ColumnFamilies".equals(metricName.getGroup())
                                       && "LiveDiskSpaceUsed".equals(metricName.getName());
                    }
                });
                for (SortedMap<MetricName, Metric> metrics : columnFamilyMetrics.values())
                {
                    for (Metric liveDiskSpaceUsed : metrics.values())
                        bytes += ((Gauge<Long>) liveDiskSpaceUsed).value();
                }
                return bytes;
            }
        });
    }
}

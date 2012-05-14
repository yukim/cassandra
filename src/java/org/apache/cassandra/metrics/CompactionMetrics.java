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
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.compaction.CompactionManager;

/**
 * Metrics for compaction.
 */
public class CompactionMetrics
{
    public static final String GROUP_NAME = "org.apache.cassandra.metrics";
    public static final String TYPE_NAME = "Compaction";

    /** Estimated number of compactions remaining to perform */
    public final Gauge<Integer> pendingTasks;
    /** Number of completed compactions since server [re]start */
    public final Gauge<Long> completedTasks;
    /** Total number of compactions since server [re]start */
    public final Gauge<Long> totalCompactionsCompleted;
    /** Total number of bytes compacted since server [re]start */
    public final Gauge<Long> totalBytesCompacted;

    public CompactionMetrics(final CompactionManager.CompactionExecutorStatsCollector... collectors)
    {
        pendingTasks = Metrics.newGauge(new MetricName(GROUP_NAME, TYPE_NAME, "PendingTasks"), new Gauge<Integer>()
        {
            public Integer value()
            {
                int n = 0;
                for (String tableName : Schema.instance.getTables())
                {
                    for (ColumnFamilyStore cfs : Table.open(tableName).getColumnFamilyStores())
                        n += cfs.getCompactionStrategy().getEstimatedRemainingTasks();
                }
                for (CompactionManager.CompactionExecutorStatsCollector collector : collectors)
                    n += collector.getTaskCount() - collector.getCompletedTaskCount();
                return n;
            }
        });
        completedTasks = Metrics.newGauge(new MetricName(GROUP_NAME, TYPE_NAME, "CompletedTasks"), new Gauge<Long>()
        {
            public Long value()
            {
                long completedTasks = 0;
                for (CompactionManager.CompactionExecutorStatsCollector collector : collectors)
                    completedTasks += collector.getCompletedTaskCount();
                return completedTasks;
            }
        });
        totalCompactionsCompleted = Metrics.newGauge(new MetricName(GROUP_NAME, TYPE_NAME, "TotalCompactionsCompleted"), new Gauge<Long>()
        {
            public Long value()
            {
                long compactionCompleted = 0;
                for (CompactionManager.CompactionExecutorStatsCollector collector : collectors)
                    compactionCompleted += collector.getTotalCompactionsCompleted();
                return compactionCompleted;
            }
        });
        totalBytesCompacted = Metrics.newGauge(new MetricName(GROUP_NAME, TYPE_NAME, "TotalBytesCompacted"), new Gauge<Long>()
        {
            public Long value()
            {
                long bytesCompacted = 0;
                for (CompactionManager.CompactionExecutorStatsCollector collector : collectors)
                    bytesCompacted += collector.getTotalBytesCompacted();
                return bytesCompacted;
            }
        });
    }
}

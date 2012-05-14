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

import org.apache.cassandra.net.MessagingService;

/**
 * Metrics for dropped messages by verb.
 */
public class DroppedMessageMetrics
{
    public static final String GROUP_NAME = "org.apache.cassandra.metrics";
    public static final String TYPE_NAME = "DroppedMessage";

    /** Number of dropped messages */
    public final Counter dropped;
    /** Number of dropped messages since last read */
    public final Gauge<Long> recentlyDropped;

    private long lastDropped = 0;

    public DroppedMessageMetrics(MessagingService.Verb verb)
    {
        dropped = Metrics.newCounter(new MetricName(GROUP_NAME, TYPE_NAME, "Dropped", verb.toString()));
        recentlyDropped = Metrics.newGauge(new MetricName(GROUP_NAME, TYPE_NAME, "RecentlyDropped", verb.toString()), new Gauge<Long>()
        {
            public Long value()
            {
                long currentDropped = dropped.count();
                long recentlyDropped = currentDropped- lastDropped;
                lastDropped = currentDropped;
                return recentlyDropped;
            }
        });
    }
}

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

import java.net.InetAddress;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

import org.apache.cassandra.net.OutboundTcpConnectionPool;

/**
 * Metrics for {@link OutboundTcpConnectionPool}.
 */
public class ConnectionMetrics
{
    public static final String GROUP_NAME = "org.apache.cassandra.metrics";
    public static final String TYPE_NAME = "Connection";

    /** Total number of timeouts happened on this node */
    public static final Counter totalTimeouts = Metrics.newCounter(new MetricName(GROUP_NAME, TYPE_NAME, "TotalTimeouts"));
    /** Number of timeouts since last check. */
    public static final Gauge<Long> recentTotalTimeouts = Metrics.newGauge(new MetricName(GROUP_NAME, TYPE_NAME, "RecentTotalTimeouts"), new Gauge<Long>()
    {
        public Long value()
        {
            long total = totalTimeouts.count();
            long recent = total - recentTimeouts;
            recentTimeouts = total;
            return recent;
        }
    });
    private static long recentTimeouts;

    public final String address;
    /** Pending tasks for Command(Mutations, Read etc) TCP Connections */
    public final Gauge<Integer> commandPendingTasks;
    /** Completed tasks for Command(Mutations, Read etc) TCP Connections */
    public final Gauge<Long> commandCompletedTasks;
    /** Dropped tasks for Command(Mutations, Read etc) TCP Connections */
    public final Gauge<Long> commandDroppedTasks;
    /** Pending tasks for Response(GOSSIP & RESPONSE) TCP Connections */
    public final Gauge<Integer> responsePendingTasks;
    /** Completed tasks for Response(GOSSIP & RESPONSE) TCP Connections */
    public final Gauge<Long> responseCompletedTasks;
    /** Number of timeouts for specific IP */
    public final Counter timeout;
    /** Number of timeouts for specific IP since last check. */
    public final Gauge<Long> recentTimeout;

    private long recentTimeoutCount;

    /**
     * Create metrics for given connection pool.
     *
     * @param ip IP address to use for metrics label
     * @param connectionPool Connection pool
     */
    public ConnectionMetrics(InetAddress ip, final OutboundTcpConnectionPool connectionPool)
    {
        address = ip.getHostAddress();
        commandPendingTasks = Metrics.newGauge(new MetricName(GROUP_NAME, TYPE_NAME, "CommandPendingTasks", address), new Gauge<Integer>()
        {
            public Integer value()
            {
                return connectionPool.cmdCon.getPendingMessages();
            }
        });
        commandCompletedTasks = Metrics.newGauge(new MetricName(GROUP_NAME, TYPE_NAME, "CommandCompletedTasks", address), new Gauge<Long>()
        {
            public Long value()
            {
                return connectionPool.cmdCon.getCompletedMesssages();
            }
        });
        commandDroppedTasks = Metrics.newGauge(new MetricName(GROUP_NAME, TYPE_NAME, "CommandDroppedTasks", address), new Gauge<Long>()
        {
            public Long value()
            {
                return connectionPool.cmdCon.getDroppedMessages();
            }
        });
        responsePendingTasks = Metrics.newGauge(new MetricName(GROUP_NAME, TYPE_NAME, "ResponsePendingTasks", address), new Gauge<Integer>()
        {
            public Integer value()
            {
                return connectionPool.ackCon.getPendingMessages();
            }
        });
        responseCompletedTasks = Metrics.newGauge(new MetricName(GROUP_NAME, TYPE_NAME, "ResponseCompletedTasks", address), new Gauge<Long>()
        {
            public Long value()
            {
                return connectionPool.ackCon.getCompletedMesssages();
            }
        });
        timeout = Metrics.newCounter(new MetricName(GROUP_NAME, TYPE_NAME, "Timeout", address));
        recentTimeout = Metrics.newGauge(new MetricName(GROUP_NAME, TYPE_NAME, "RecentTimeout", address), new Gauge<Long>()
        {
            public Long value()
            {
                long timeoutCount = timeout.count();
                long recent = timeoutCount - recentTimeoutCount;
                recentTimeoutCount = timeoutCount;
                return recent;
            }
        });
    }

    public void release()
    {
        Metrics.defaultRegistry().removeMetric(new MetricName(GROUP_NAME, TYPE_NAME, "CommandPendingTasks", address));
        Metrics.defaultRegistry().removeMetric(new MetricName(GROUP_NAME, TYPE_NAME, "CommandCompletedTasks", address));
        Metrics.defaultRegistry().removeMetric(new MetricName(GROUP_NAME, TYPE_NAME, "CommandDroppedTasks", address));
        Metrics.defaultRegistry().removeMetric(new MetricName(GROUP_NAME, TYPE_NAME, "ResponsePendingTasks", address));
        Metrics.defaultRegistry().removeMetric(new MetricName(GROUP_NAME, TYPE_NAME, "ResponseCompletedTasks", address));
        Metrics.defaultRegistry().removeMetric(new MetricName(GROUP_NAME, TYPE_NAME, "Timeout", address));
        Metrics.defaultRegistry().removeMetric(new MetricName(GROUP_NAME, TYPE_NAME, "RecentTimeout", address));
    }
}

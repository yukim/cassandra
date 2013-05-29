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
package org.apache.cassandra.streaming;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.util.concurrent.RateLimiter;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.cliffc.high_scale_lib.NonBlockingHashSet;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.streaming.management.StreamEventHandler;
import org.apache.cassandra.streaming.management.StreamingStatusManager;
import org.apache.cassandra.utils.UUIDGen;

/**
 * StreamManager manages current running operation and provides status of all operation invoked.
 *
 * All stream operation should be created through this class to track streaming status and progress.
 */
public class StreamManager
{
    public static final StreamManager instance = new StreamManager();

    /** Currently running operations. Operations are removed after completion/failure. */
    private final Set<StreamOperation> currentOperations = new NonBlockingHashSet<>();

    private final Map<InetAddress, RateLimiter> rateLimiters = new NonBlockingHashMap<>();

    private final StreamEventHandler[] eventHandlers = new StreamEventHandler[]{new StreamingStatusManager()};

    private StreamManager()
    {
    }

    /**
     * Gets streaming rate limiter. When stream_throughput_outbound_megabits_per_sec is 0,
     * this returns rate limiter with the rate of Double.MAX_VALUE bytes per second.
     * Rate unit is bytes per sec.
     *
     * @param address address to apply RateLimiter
     * @return RateLimiter with rate limit set
     */
    public RateLimiter getRateLimiter(InetAddress address)
    {
        RateLimiter limiter = rateLimiters.get(address);
        if (limiter == null)
        {
            limiter = RateLimiter.create(Double.MAX_VALUE);
            rateLimiters.put(address, limiter);
        }
        double currentThroughput = DatabaseDescriptor.getStreamThroughputOutboundMegabitsPerSec() * 1024 * 1024 / 8 / 1000;
        // if throughput is set to 0, throttling is disabled
        if (currentThroughput == 0)
            currentThroughput = Double.MAX_VALUE;
        if (limiter.getRate() != currentThroughput)
            limiter.setRate(currentThroughput);
        return limiter;
    }

    public StreamOperation createOperation(OperationType type)
    {
        return createOperation(UUIDGen.getTimeUUID(), type);
    }

    public StreamOperation createOperation(UUID operationId, OperationType type)
    {
        StreamOperation op = new StreamOperation(operationId, type);
        for (StreamEventHandler handler : eventHandlers)
            op.addEventListener(handler);

        Gossiper.instance.register(op);
        FailureDetector.instance.registerFailureDetectionEventListener(op);

        currentOperations.add(op);
        return op;
    }
}

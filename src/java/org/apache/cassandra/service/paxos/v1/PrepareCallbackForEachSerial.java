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

package org.apache.cassandra.service.paxos.v1;


import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.PrepareResponse;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class PrepareCallbackForEachSerial extends AbstractPrepareCallback
{
    private final Map<String, AtomicInteger> responses = new HashMap<>();
    private final AtomicInteger acks = new AtomicInteger(0);
    private final IEndpointSnitch snitch;
    private final Condition condition = Condition.newOneTimeCondition();

    public PrepareCallbackForEachSerial(DecoratedKey key, TableMetadata metadata, ReplicaPlan.ForPaxosWrite replicaPlan,
                                        long queryStartNanoTime, IEndpointSnitch snitch)
    {
        super(key, metadata, replicaPlan, queryStartNanoTime);
        this.snitch = snitch;

        if (replicaPlan.replicationStrategy() instanceof NetworkTopologyStrategy)
        {
            NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) replicaPlan.replicationStrategy();
            for (String dc : strategy.getDatacenters())
            {
                int rf = strategy.getReplicationFactor(dc).allReplicas;
                responses.put(dc, new AtomicInteger((rf / 2) + 1));
            }
        }
    }

    @Override
    public void signalWhenReady(Message<PrepareResponse> message)
    {
        String dc = snitch.getDatacenter(message.from());
        responses.get(dc).getAndDecrement();
        acks.incrementAndGet();
        for (AtomicInteger i : responses.values())
        {
            if (i.get() > 0)
                return;
        }
        signal();
    }

    @Override
    protected void signal()
    {
        condition.signalAll();
    }

    public void await(long timeout) throws WriteTimeoutException
    {
        try
        {
            if (!condition.await(timeout, NANOSECONDS))
            {
                throw new WriteTimeoutException(WriteType.CAS, replicaPlan.consistencyLevel(),
                        acks.get(), replicaPlan.requiredParticipants());
            }
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
    }
}

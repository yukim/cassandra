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


import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ProposeCallback for EACH_SERIAL
 */
public class ProposeCallbackForEachSerial extends AbstractProposeCallback
{
    private static final Logger logger = LoggerFactory.getLogger(ProposeCallbackForEachSerial.class);

    private final Map<String, AtomicInteger> requiredResponses = new HashMap<>();
    private final Map<String, AtomicInteger> accepts = new HashMap<>();
    private final IEndpointSnitch snitch;
    private final Condition condition = Condition.newOneTimeCondition();

    public ProposeCallbackForEachSerial(ReplicaPlan.ForPaxosWrite replicaPlan, boolean failFast, long queryStartNanoTime,
                                        IEndpointSnitch snitch)
    {
        super(replicaPlan, failFast, queryStartNanoTime);
        this.snitch = snitch;

        if (replicaPlan.consistencyLevel() != ConsistencyLevel.EACH_SERIAL)
        {
            throw new AssertionError("Consistency level was not EACH_SERIAL, but was " + replicaPlan.consistencyLevel());
        }

        if (replicaPlan.replicationStrategy() instanceof NetworkTopologyStrategy)
        {
            NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) replicaPlan.replicationStrategy();
            for (String dc : strategy.getDatacenters())
            {
                int rf = strategy.getReplicationFactor(dc).allReplicas;
                requiredResponses.put(dc, new AtomicInteger((rf / 2) + 1));
                accepts.put(dc, new AtomicInteger(0));
            }
        }
        else
        {
            throw new AssertionError("Replication strategy should be NetworkTopologyStrategy in order to use EACH_SERIAL");
        }
    }

    public void onResponse(Message<Boolean> msg)
    {
        logger.trace("Propose response {} from {}", msg.payload, msg.from());
        String dc = snitch.getDatacenter(msg.from());

        if (msg.payload)
        {
            accepts.get(dc).incrementAndGet();
        }

        requiredResponses.get(dc).decrementAndGet();

        if (isSuccessful() || failFast())
        {
            condition.signalAll();
        }
    }

    @Override
    protected void await(long timeout) throws WriteTimeoutException
    {
        try
        {
            if (!condition.await(timeout, TimeUnit.MILLISECONDS))
            {
                int totalRequiredResponses = requiredResponses.values().stream().mapToInt(AtomicInteger::get).sum();
                throw new WriteTimeoutException(WriteType.CAS, replicaPlan.consistencyLevel(),
                        replicaPlan.contacts().size() - totalRequiredResponses,
                        replicaPlan.requiredParticipants());
            }
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
    }

    @Override
    public int getAcceptCount()
    {

        return accepts.values().stream().mapToInt(AtomicInteger::get).sum();
    }

    /**
     * For EACH_SERIAL consistency, quorum of replica need to accept from
     * each DC.
     *
     * @return true if quorum of replica from each DC accepted the proposal.
     */
    @Override
    public boolean isSuccessful()
    {
        boolean success = true;
        for (Map.Entry<String, AtomicInteger> accept : accepts.entrySet())
        {
            success &= accept.getValue().get() >= requiredResponses.get(accept.getKey()).get();
        }
        return success;
    }

    @Override
    public boolean isFullyRefused()
    {
        boolean fullyRefused = true;
        for (Map.Entry<String, AtomicInteger> response : requiredResponses.entrySet())
        {
            fullyRefused &= response.getValue().get() == 0 && accepts.get(response.getKey()).get() == 0;
        }
        return fullyRefused;
    }

    /**
     * For EACH_SERIAL consistency, when quorum of replica in *ANY* DC rejected,
     * fail fast in failFast mode.
     *
     * @return true when quorum of replica in any DC
     */
    private boolean failFast()
    {
        if (failFast)
        {
            NetworkTopologyStrategy strategy = (NetworkTopologyStrategy) replicaPlan.replicationStrategy();
            for (String dc : strategy.getDatacenters())
            {
                int rf = strategy.getReplicationFactor(dc).allReplicas;
                return requiredResponses.get(dc).get() + accepts.get(dc).get() < (rf / 2) + 1;
            }
        }
        return false;
    }
}

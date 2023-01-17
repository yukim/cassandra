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


import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.Nemesis;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ProposeCallback has two modes of operation, controlled by the failFast parameter.
 *
 * In failFast mode, we will return a failure as soon as a majority of nodes reject
 * the proposal. This is used when replaying a proposal from an earlier leader.
 *
 * Otherwise, we wait for either all replicas to respond or until we achieve
 * the desired quorum. We continue to wait for all replicas even after we know we cannot succeed
 * because we need to know if no node at all have accepted or if at least one has.
 * In the former case, a proposer is guaranteed no-one will
 * replay its value; in the latter we don't, so we must timeout in case another
 * leader replays it before we can; see CASSANDRA-6013
 */
public abstract class AbstractProposeCallback extends AbstractPaxosCallback<Boolean>
{
    protected final boolean failFast;

    public AbstractProposeCallback(ReplicaPlan.ForPaxosWrite replicaPlan, boolean failFast, long queryStartNanoTime)
    {
        super(replicaPlan, queryStartNanoTime);
        this.failFast = failFast;
    }

    public abstract int getAcceptCount();

    public abstract boolean isSuccessful();

    // Note: this is only reliable if !failFast
    public abstract boolean isFullyRefused();
}

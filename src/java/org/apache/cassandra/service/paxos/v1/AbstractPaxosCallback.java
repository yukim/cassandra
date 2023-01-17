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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.RequestCallback;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public abstract class AbstractPaxosCallback<T> implements RequestCallback<T>
{
    protected final ReplicaPlan.ForPaxosWrite replicaPlan;
    private final long queryStartNanoTime;

    public AbstractPaxosCallback(ReplicaPlan.ForPaxosWrite replicaPlan, long queryStartNanoTime)
    {
        this.replicaPlan = replicaPlan;
        this.queryStartNanoTime = queryStartNanoTime;
    }

    public void await() throws WriteTimeoutException
    {
        long timeout = DatabaseDescriptor.getWriteRpcTimeout(NANOSECONDS) - (nanoTime() - queryStartNanoTime);
        await(timeout);
    }

    protected abstract void await(long timeout) throws WriteTimeoutException;
}

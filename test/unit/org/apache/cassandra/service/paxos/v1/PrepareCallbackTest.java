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

import org.apache.cassandra.Util;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.service.paxos.PrepareResponse;
import org.junit.Test;

import java.net.UnknownHostException;

import static org.apache.cassandra.service.paxos.Ballot.Flag.GLOBAL;
import static org.apache.cassandra.service.paxos.BallotGenerator.Global.nextBallot;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PrepareCallbackTest extends AbstractPrepareCallbackTest
{
    @Test
    public void testSufficientResponsesForSerial() throws UnknownHostException
    {
        // Prepare message
        DecoratedKey key = Util.dk("a");
        Keyspace ks = Keyspace.open(KEYSPACE);
        Ballot ballot = nextBallot(GLOBAL);
        TableMetadata tableMetadata = ks.getMetadata().tables.getNullable(TABLE);
        Commit toPrepare = Commit.newPrepare(key, tableMetadata, ballot);

        ReplicaPlan.ForPaxosWrite replicaPlan = ReplicaPlans.forPaxos(ks, key, ConsistencyLevel.SERIAL);
        PrepareCallback callback = new PrepareCallback(toPrepare.update.partitionKey(),
                toPrepare.update.metadata(), replicaPlan, 100);

        PrepareResponse response = PaxosState.legacyPrepare(toPrepare);
        Message.Builder<PrepareResponse> msg = Message.builder(Verb.PAXOS_PREPARE_RSP, response);
        // SERIAL requires 4 responses in this keyspace to be successful
        callback.onResponse(msg.from(InetAddressAndPort.getByName("127.1.0.2")).build());
        assertTrue(callback.promised);
        callback.onResponse(msg.from(InetAddressAndPort.getByName("127.1.0.3")).build());
        assertTrue(callback.promised);
        callback.onResponse(msg.from(InetAddressAndPort.getByName("127.2.0.1")).build());
        assertTrue(callback.promised);
        callback.onResponse(msg.from(InetAddressAndPort.getByName("127.2.0.2")).build());
        assertTrue(callback.promised);

        try
        {
            callback.await(0);
        }
        catch (WriteTimeoutException e)
        {
            fail("WriteTimeoutException shuold not be thrown");
        }
    }

    @Test
    public void testSufficientResponsesForLocalSerial() throws UnknownHostException
    {
        // Prepare message
        DecoratedKey key = Util.dk("a");
        Keyspace ks = Keyspace.open(KEYSPACE);
        Ballot ballot = nextBallot(GLOBAL);
        TableMetadata tableMetadata = ks.getMetadata().tables.getNullable(TABLE);
        Commit toPrepare = Commit.newPrepare(key, tableMetadata, ballot);

        ReplicaPlan.ForPaxosWrite replicaPlan = ReplicaPlans.forPaxos(ks, key, ConsistencyLevel.LOCAL_SERIAL);
        PrepareCallback callback = new PrepareCallback(toPrepare.update.partitionKey(),
                toPrepare.update.metadata(), replicaPlan, 100);

        PrepareResponse response = PaxosState.legacyPrepare(toPrepare);
        Message.Builder<PrepareResponse> msg = Message.builder(Verb.PAXOS_PREPARE_RSP, response);
        // LOCAL_SERIAL requires 2 responses in dc1 to be successful
        callback.onResponse(msg.from(InetAddressAndPort.getByName("127.1.0.2")).build());
        assertTrue(callback.promised);
        callback.onResponse(msg.from(InetAddressAndPort.getByName("127.1.0.3")).build());
        assertTrue(callback.promised);

        try
        {
            callback.await(0);
        }
        catch (WriteTimeoutException e)
        {
            fail("WriteTimeoutException shuold not be thrown");
        }
    }
}
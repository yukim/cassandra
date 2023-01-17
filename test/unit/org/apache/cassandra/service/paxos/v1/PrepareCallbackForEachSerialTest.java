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
import org.apache.cassandra.config.DatabaseDescriptor;
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
import java.util.Arrays;
import java.util.stream.Stream;

import static org.apache.cassandra.service.paxos.Ballot.Flag.GLOBAL;
import static org.apache.cassandra.service.paxos.BallotGenerator.Global.nextBallot;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class PrepareCallbackForEachSerialTest extends AbstractPrepareCallbackTest
{
    @Test
    public void testSufficientResponsesFromEachDC() throws UnknownHostException
    {
        DecoratedKey key = Util.dk("a");
        Keyspace ks = Keyspace.open(KEYSPACE);
        Ballot ballot = nextBallot(GLOBAL);
        TableMetadata tableMetadata = ks.getMetadata().tables.getNullable(TABLE);
        Commit toPrepare = Commit.newPrepare(key, tableMetadata, ballot);

        ReplicaPlan.ForPaxosWrite replicaPlan = ReplicaPlans.forPaxos(Keyspace.open(KEYSPACE),
                toPrepare.update.partitionKey(), ConsistencyLevel.EACH_SERIAL);
        PrepareCallbackForEachSerial callback = new PrepareCallbackForEachSerial(toPrepare.update.partitionKey(),
                toPrepare.update.metadata(), replicaPlan, 100, DatabaseDescriptor.getEndpointSnitch());

        PrepareResponse response = PaxosState.legacyPrepare(toPrepare);
        Message.Builder<PrepareResponse> msg = Message.builder(Verb.PAXOS_PREPARE_RSP, response);
        // EACH_SERIAL requires 4 responses, 2 from each DC, to be successful
        callback.onResponse(msg.from(InetAddressAndPort.getByName("127.1.0.2")).build());
        callback.onResponse(msg.from(InetAddressAndPort.getByName("127.1.0.3")).build());
        callback.onResponse(msg.from(InetAddressAndPort.getByName("127.2.0.1")).build());
        callback.onResponse(msg.from(InetAddressAndPort.getByName("127.2.0.2")).build());

        // should not throw WriteTimeoutException
        try
        {
            callback.await(0);
        }
        catch (WriteTimeoutException t)
        {
            fail("WriteTimeoutException is not expected");
        }
        assertTrue(callback.promised);
    }

    @Test
    public void testPromiseFailed() throws UnknownHostException
    {
        // EACH_SERIAL requires 4 responses, 2 from each DC, to be successful
        InetAddressAndPort[] respondants = Stream.of(new String[]{"127.1.0.2", "127.1.0.3", "127.2.0.1", "127.2.0.2"}).
                map((s) -> {
                    try {
                        return InetAddressAndPort.getByName(s);
                    }
                    catch (UnknownHostException e)
                    {
                        throw new RuntimeException(e);
                    }
        }).toArray(InetAddressAndPort[]::new);

        // should not throw WriteTimeoutException
        try
        {
            AbstractPrepareCallback callback = prepareAndWait(ConsistencyLevel.EACH_SERIAL, respondants);
            assertTrue(callback.promised);
        }
        catch (WriteTimeoutException t)
        {
            fail("WriteTimeoutException is not expected");
        }
    }

    @Test
    public void testInsufficientResponses() throws UnknownHostException
    {
        DecoratedKey key = Util.dk("a");
        Keyspace ks = Keyspace.open(KEYSPACE);
        Ballot ballot = nextBallot(GLOBAL);
        TableMetadata tableMetadata = ks.getMetadata().tables.getNullable(TABLE);
        Commit toPrepare = Commit.newPrepare(key, tableMetadata, ballot);

        ReplicaPlan.ForPaxosWrite replicaPlan = ReplicaPlans.forPaxos(ks, key, ConsistencyLevel.EACH_SERIAL);
        PrepareCallbackForEachSerial callback = new PrepareCallbackForEachSerial(toPrepare.update.partitionKey(),
                toPrepare.update.metadata(), replicaPlan, 100, DatabaseDescriptor.getEndpointSnitch());

        PrepareResponse response = PaxosState.legacyPrepare(toPrepare);
        Message.Builder<PrepareResponse> msg = Message.builder(Verb.PAXOS_PREPARE_RSP, response);
        // When using EACH_SERIAL and only recieved 1 response from "dc2"
        callback.onResponse(msg.from(InetAddressAndPort.getByName("127.1.0.1")).build());
        callback.onResponse(msg.from(InetAddressAndPort.getByName("127.1.0.2")).build());
        callback.onResponse(msg.from(InetAddressAndPort.getByName("127.1.0.3")).build());
        callback.onResponse(msg.from(InetAddressAndPort.getByName("127.2.0.1")).build());

        // should throw WriteTimeoutException
        try
        {
            callback.await(0);
            fail("WriteTimeoutException is expected");
        }
        catch (WriteTimeoutException t)
        {
            assertThat(t.blockFor, is(4));
            assertThat(t.received, is(4));
        }
    }
}
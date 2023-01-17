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

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.*;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.service.paxos.PrepareResponse;
import org.apache.cassandra.service.reads.range.TokenUpdater;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import static org.apache.cassandra.service.paxos.Ballot.Flag.GLOBAL;
import static org.apache.cassandra.service.paxos.BallotGenerator.Global.nextBallot;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AbstractPrepareCallbackTest {
    public static final String TABLE = "tbl";
    protected static final String KEYSPACE = "PrepareCallbackTest";

    @BeforeClass
    public static void setupSchema() throws UnknownHostException {
        SchemaLoader.loadSchema();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        DatabaseDescriptor.setEndpointSnitch(new Snitch());
        DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.1.0.1"));

        // Setup cluster topology
        TokenUpdater updater = new TokenUpdater();
        // dc1
        updater.withKeys(InetAddressAndPort.getByName("127.1.0.1"), 100);
        updater.withKeys(InetAddressAndPort.getByName("127.1.0.2"), 200);
        updater.withKeys(InetAddressAndPort.getByName("127.1.0.3"), 300);
        // dc2
        updater.withKeys(InetAddressAndPort.getByName("127.2.0.1"), 110);
        updater.withKeys(InetAddressAndPort.getByName("127.2.0.2"), 220);
        updater.withKeys(InetAddressAndPort.getByName("127.2.0.3"), 330);
        updater.update();

        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.1.0.1"));
        metadata.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.1.0.2"));
        metadata.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.1.0.3"));
        metadata.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.2.0.1"));
        metadata.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.2.0.2"));
        metadata.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.2.0.3"));

        SchemaLoader.createKeyspace(KEYSPACE,
                KeyspaceParams.nts("datacenter1", 3, "datacenter2", 3), SchemaLoader.standardCFMD(KEYSPACE, TABLE));
    }

    static class Snitch extends AbstractNetworkTopologySnitch {
        @Override
        public String getRack(InetAddressAndPort endpoint) {
            return "rack1";
        }

        /**
         * Returns either "datacenter1" or "datacenter2", based on the second octet of IP.
         * <p>
         * The name of the datacenter is chosen so that {@link org.apache.cassandra.locator.InOurDc} can
         * filter local DC defined in {@link DatabaseDescriptor}.
         * </p>
         * <p>
         * {@link DatabaseDescriptor} only sets {@code localDc} when {@link DatabaseDescriptor#applySnitch()} is
         * called, which is not called repeatablly because of its MBean registration behavior.
         * </p>
         */
        @Override
        public String getDatacenter(InetAddressAndPort endpoint) {
            byte[] address = endpoint.getAddress().getAddress();
            if (address[1] == 1)
                return "datacenter1";
            else
                return "datacenter2";
        }
    }

    protected AbstractPrepareCallback prepareAndWait(ConsistencyLevel consistencyLevel, InetAddressAndPort... respondants) throws WriteTimeoutException
    {
        DecoratedKey key = Util.dk("a");
        Keyspace ks = Keyspace.open(KEYSPACE);
        Ballot ballot = nextBallot(GLOBAL);
        TableMetadata tableMetadata = ks.getMetadata().tables.getNullable(TABLE);
        Commit toPrepare = Commit.newPrepare(key, tableMetadata, ballot);

        ReplicaPlan.ForPaxosWrite replicaPlan = ReplicaPlans.forPaxos(Keyspace.open(KEYSPACE),
                toPrepare.update.partitionKey(), consistencyLevel);
        int queryStartNanoTime = 100;
        AbstractPrepareCallback callback = consistencyLevel == ConsistencyLevel.EACH_SERIAL ?
                new PrepareCallbackForEachSerial(toPrepare.update.partitionKey(), toPrepare.update.metadata(),
                        replicaPlan, queryStartNanoTime, DatabaseDescriptor.getEndpointSnitch())
                : new PrepareCallback(key, toPrepare.update.metadata(), replicaPlan, queryStartNanoTime);

        PrepareResponse response = PaxosState.legacyPrepare(toPrepare);
        Message.Builder<PrepareResponse> msg = Message.builder(Verb.PAXOS_PREPARE_RSP, response);
        // EACH_SERIAL requires 4 responses, 2 from each DC, to be successful
        for (InetAddressAndPort respondant : respondants) {
            callback.onResponse(msg.from(respondant).build());
        }

        callback.await(0);
        return callback;
    }
}

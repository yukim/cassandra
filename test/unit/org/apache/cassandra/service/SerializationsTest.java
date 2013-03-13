/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.service;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.AbstractSerializationsTester;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.TreeRequest;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.repair.ValidatorSerializer;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;

public class SerializationsTest extends AbstractSerializationsTester
{
    static
    {
        System.setProperty("cassandra.partitioner", "RandomPartitioner");
    }

    public static Range<Token> FULL_RANGE = new Range<Token>(StorageService.getPartitioner().getMinimumToken(), StorageService.getPartitioner().getMinimumToken());

    private void testTreeRequestWrite() throws IOException
    {
        DataOutputStream out = getOutput("service.TreeRequest.bin");
        TreeRequest.serializer.serialize(Statics.req, out, getVersion());
        Statics.req.createMessage().serialize(out, getVersion());
        out.close();

        // test serializedSize
        testSerializedSize(Statics.req, TreeRequest.serializer);
    }

    @Test
    public void testTreeRequestRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testTreeRequestWrite();

        DataInputStream in = getInput("service.TreeRequest.bin");
        assert TreeRequest.serializer.deserialize(in, getVersion()) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        in.close();
    }

    private void testTreeResponseWrite() throws IOException
    {
        // empty validation
        Validator v0 = new Validator(Statics.desc, FBUtilities.getBroadcastAddress(),  -1);

        // validation with a tree
        IPartitioner p = new RandomPartitioner();
        MerkleTree mt = new MerkleTree(p, FULL_RANGE, MerkleTree.RECOMMENDED_DEPTH, Integer.MAX_VALUE);
        for (int i = 0; i < 10; i++)
            mt.split(p.getRandomToken());
        Validator v1 = new Validator(Statics.desc, FBUtilities.getBroadcastAddress(), mt, -1);

        DataOutputStream out = getOutput("service.TreeResponse.bin");
        ValidatorSerializer.instance.serialize(v0, out, getVersion());
        ValidatorSerializer.instance.serialize(v1, out, getVersion());
        v0.createMessage().serialize(out, getVersion());
        v1.createMessage().serialize(out, getVersion());
        out.close();

        // test serializedSize
        testSerializedSize(v0, ValidatorSerializer.instance);
        testSerializedSize(v1, ValidatorSerializer.instance);
    }

    @Test
    public void testTreeResponseRead() throws IOException
    {
        if (EXECUTE_WRITES)
            testTreeResponseWrite();

        DataInputStream in = getInput("service.TreeResponse.bin");
        assert ValidatorSerializer.instance.deserialize(in, getVersion()) != null;
        assert ValidatorSerializer.instance.deserialize(in, getVersion()) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        assert MessageIn.read(in, getVersion(), -1) != null;
        in.close();
    }

    private static class Statics
    {
        private static final RepairJobDesc desc = new RepairJobDesc(UUID.randomUUID(), "Keyspace1", "Standard1", FULL_RANGE);
        private static final TreeRequest req = new TreeRequest(desc, FBUtilities.getBroadcastAddress(), 1234);
    }
}

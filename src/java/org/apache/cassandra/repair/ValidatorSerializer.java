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
package org.apache.cassandra.repair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;

/**
 * ValidatorSerializer is used to respond merkle tree to the nodes version older than 2.0.
 */
@Deprecated
public class ValidatorSerializer implements IVersionedSerializer<Validator>
{
    public static final IVersionedSerializer<Validator> instance = new ValidatorSerializer();

    public void serialize(Validator validator, DataOutput out, int version) throws IOException
    {
        TreeRequest request = new TreeRequest(validator.desc, FBUtilities.getBroadcastAddress(), validator.gcBefore);
        TreeRequest.serializer.serialize(request, out, version);
        MerkleTree.serializer.serialize(validator.tree, out, version);
    }

    public Validator deserialize(DataInput in, int version) throws IOException
    {
        final TreeRequest request = TreeRequest.serializer.deserialize(in, version);
        try
        {
            return new Validator(request.desc, request.endpoint, MerkleTree.serializer.deserialize(in, version), request.gcBefore);
        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public long serializedSize(Validator validator, int version)
    {
        TreeRequest request = new TreeRequest(validator.desc, FBUtilities.getBroadcastAddress(), validator.gcBefore);
        return TreeRequest.serializer.serializedSize(request, version)
               + MerkleTree.serializer.serializedSize(validator.tree, version);
    }
}

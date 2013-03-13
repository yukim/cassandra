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
import java.net.InetAddress;
import java.util.UUID;

import com.google.common.base.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDGen;

/**
 * TreeRequest is a message sent to other endpoints to perform validation.
 */
@Deprecated
public class TreeRequest
{
    public static final TreeRequestSerializer serializer = new TreeRequestSerializer();

    public final RepairJobDesc desc;
    /** repair initiator's endpoint address */
    public final InetAddress endpoint;
    public final int gcBefore;

    public TreeRequest(RepairJobDesc desc, InetAddress endpoint, int gcBefore)
    {
        this.desc = desc;
        this.endpoint = endpoint;
        this.gcBefore = gcBefore;
    }

    @Override
    public final int hashCode()
    {
        return Objects.hashCode(desc, endpoint);
    }

    @Override
    public final boolean equals(Object o)
    {
        if (!(o instanceof TreeRequest))
            return false;
        TreeRequest that = (TreeRequest)o;
        // handles nulls properly
        return Objects.equal(desc, that.desc) && Objects.equal(endpoint, that.endpoint);
    }

    public MessageOut<TreeRequest> createMessage()
    {
        return new MessageOut<>(MessagingService.Verb.TREE_REQUEST, this, TreeRequest.serializer);
    }

    @Deprecated
    public static class TreeRequestSerializer implements IVersionedSerializer<TreeRequest>
    {
        public void serialize(TreeRequest request, DataOutput out, int version) throws IOException
        {
            out.writeUTF(request.desc.sessionId.toString());
            CompactEndpointSerializationHelper.serialize(request.endpoint, out);
            if (version >= MessagingService.VERSION_20)
                out.writeInt(request.gcBefore);
            out.writeUTF(request.desc.keyspace);
            out.writeUTF(request.desc.columnFamily);
            AbstractBounds.serializer.serialize(request.desc.range, out, version);
        }

        public TreeRequest deserialize(DataInput in, int version) throws IOException
        {
            String sessId = in.readUTF();
            // older version sends UUID in string format
            UUID id;
            try
            {
                id = UUID.fromString(sessId);
            }
            catch (Exception e)
            {
                // Some tests use String that is not UUID
                id = UUIDGen.getTimeUUID();
            }
            InetAddress endpoint = CompactEndpointSerializationHelper.deserialize(in);
            int gcBefore = -1;
            if (version >= MessagingService.VERSION_20)
                gcBefore = in.readInt();
            String keyspace = in.readUTF();
            String columnFamily = in.readUTF();
            Range<Token> range = (Range<Token>) AbstractBounds.serializer.deserialize(in, version);
            return new TreeRequest(new RepairJobDesc(id, keyspace, columnFamily, range), endpoint, gcBefore);
        }

        public long serializedSize(TreeRequest request, int version)
        {
            long size = TypeSizes.NATIVE.sizeof(request.desc.sessionId.toString());
            size += CompactEndpointSerializationHelper.serializedSize(request.endpoint);
            size += TypeSizes.NATIVE.sizeof(request.desc.keyspace);
            size += TypeSizes.NATIVE.sizeof(request.desc.columnFamily);
            size += AbstractBounds.serializer.serializedSize(request.desc.range, version);
            return size;
        }
    }
}

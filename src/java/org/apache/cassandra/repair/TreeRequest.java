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

import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

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

    public TreeRequest(RepairJobDesc desc, InetAddress endpoint)
    {
        this.desc = desc;
        this.endpoint = endpoint;
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

    public static class TreeRequestSerializer implements IVersionedSerializer<TreeRequest>
    {
        public void serialize(TreeRequest request, DataOutput dos, int version) throws IOException
        {
            throw new UnsupportedOperationException("this version does not serialize TreeRequest");
        }

        public TreeRequest deserialize(DataInput dis, int version) throws IOException
        {
            assert version < MessagingService.VERSION_20;
            String sessId = dis.readUTF();
            // older version sends UUID in string format
            UUID id = UUID.fromString(sessId);
            InetAddress endpoint = CompactEndpointSerializationHelper.deserialize(dis);
            String keyspace = dis.readUTF();
            String columnFamily = dis.readUTF();
            Range<Token> range = (Range<Token>) AbstractBounds.serializer.deserialize(dis, version);
            return new TreeRequest(new RepairJobDesc(id, keyspace, columnFamily, range), endpoint);
        }

        public long serializedSize(TreeRequest request, int version)
        {
            throw new UnsupportedOperationException("this version does not serialize TreeRequest");
        }
    }
}

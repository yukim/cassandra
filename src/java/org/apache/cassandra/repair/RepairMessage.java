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
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.MerkleTree;

/**
 * All repair request/response messages are consist of RepairMessageHeader and the content
 * that specific to each RepairMessageType.
 *
 * @since 2.0
 */
public class RepairMessage<T>
{
    public static final IVersionedSerializer<RepairMessage<?>> serializer = new RepairMessageSerializer();
    private static final Map<RepairMessageType, IVersionedSerializer> bodySerializers = new HashMap<>();
    static
    {
        bodySerializers.put(RepairMessageType.VALIDATION_COMPLETE, MerkleTree.serializer);
        bodySerializers.put(RepairMessageType.SYNC_REQUEST, SyncRequest.serializer);
        bodySerializers.put(RepairMessageType.SYNC_COMPLETE, NodePair.serializer);
        bodySerializers.put(RepairMessageType.SYNC_FAILED, NodePair.serializer);
    }

    public final RepairMessageHeader header;

    public final T body;

    public RepairMessage(RepairMessageHeader header)
    {
        this(header, null);
    }

    public RepairMessage(RepairMessageHeader header, T body)
    {
        this.header = header;
        this.body = body;
    }

    public MessageOut<RepairMessage<?>> createMessage()
    {
        return new MessageOut<>(MessagingService.Verb.REPAIR_MESSAGE, this, RepairMessage.serializer);
    }

    public static class RepairMessageSerializer<T2> implements IVersionedSerializer<RepairMessage<T2>>
    {
        public void serialize(RepairMessage<T2> message, DataOutput out, int version) throws IOException
        {
            RepairMessageHeader.serializer.serialize(message.header, out, version);
            if (message.body != null)
            {
                IVersionedSerializer<T2> bodySerializer = bodySerializers.get(message.header.type);
                assert bodySerializer != null : "unknown repair message type";
                bodySerializer.serialize(message.body, out, version);
            }
        }

        public RepairMessage<T2> deserialize(DataInput in, int version) throws IOException
        {
            RepairMessageHeader header = RepairMessageHeader.serializer.deserialize(in, version);
            IVersionedSerializer<T2> bodySerializer = bodySerializers.get(header.type);
            T2 body = null;
            if (bodySerializer != null)
                body = bodySerializer.deserialize(in, version);
            return new RepairMessage<>(header, body);
        }

        public long serializedSize(RepairMessage<T2> message, int version)
        {
            long size = 0;
            size += RepairMessageHeader.serializer.serializedSize(message.header, version);
            if (message.body != null)
            {
                IVersionedSerializer<T2> bodySerializer = bodySerializers.get(message.header.type);
                assert bodySerializer != null : "unknown repair message type";
                size += bodySerializer.serializedSize(message.body, version);
            }
            return size;
        }
    }
}

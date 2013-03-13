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
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.utils.UUIDSerializer;

/**
 * RepairMessageHeader is added to each RepairMessage to indicate which RepairJob this message is sent for.
 *
 * @since 2.0
 */
public class RepairMessageHeader
{
    public static final IVersionedSerializer<RepairMessageHeader> serializer = new RepairMessageHeaderSerializer();

    public final RepairJobDesc desc;
    public final RepairMessageType type;

    public RepairMessageHeader(RepairJobDesc desc, RepairMessageType type)
    {
        this.desc = desc;
        this.type = type;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RepairMessageHeader that = (RepairMessageHeader) o;
        return desc.equals(that.desc) && type == that.type;
    }

    @Override
    public int hashCode()
    {
        int result = desc.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    public static class RepairMessageHeaderSerializer implements IVersionedSerializer<RepairMessageHeader>
    {
        public void serialize(RepairMessageHeader header, DataOutput out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(header.desc.sessionId, out, version);
            out.writeUTF(header.desc.keyspace);
            out.writeUTF(header.desc.columnFamily);
            AbstractBounds.serializer.serialize(header.desc.range, out, version);
            out.writeInt(header.type.ordinal());
        }

        public RepairMessageHeader deserialize(DataInput in, int version) throws IOException
        {
            UUID sessionId = UUIDSerializer.serializer.deserialize(in, version);
            String keyspace = in.readUTF();
            String columnFamily = in.readUTF();
            Range<Token> range = (Range<Token>)AbstractBounds.serializer.deserialize(in, version);
            RepairMessageType type = RepairMessageType.values()[in.readInt()];
            return new RepairMessageHeader(new RepairJobDesc(sessionId, keyspace, columnFamily, range), type);
        }

        public long serializedSize(RepairMessageHeader header, int version)
        {
            int size = 0;
            size += UUIDSerializer.serializer.serializedSize(header.desc.sessionId, version);
            size += TypeSizes.NATIVE.sizeof(header.desc.keyspace);
            size += TypeSizes.NATIVE.sizeof(header.desc.columnFamily);
            size += AbstractBounds.serializer.serializedSize(header.desc.range, version);
            size += TypeSizes.NATIVE.sizeof(header.type.ordinal());
            return size;
        }
    }
}

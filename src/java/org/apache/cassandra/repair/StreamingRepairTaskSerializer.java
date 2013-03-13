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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDSerializer;

/**
 * Serializer used to deserialize message from the nodes which version is older than 2.0.
 */
@Deprecated
public class StreamingRepairTaskSerializer implements IVersionedSerializer<StreamingRepairTask>
{
    public static IVersionedSerializer<StreamingRepairTask> serializer = new StreamingRepairTaskSerializer();

    private StreamingRepairTaskSerializer() {}

    public void serialize(StreamingRepairTask task, DataOutput out, int version) throws IOException
    {
        throw new UnsupportedOperationException("this version does not serialize StreamingRepairTask");
    }

    public StreamingRepairTask deserialize(DataInput in, int version) throws IOException
    {
        assert version < MessagingService.VERSION_20;
        UUID id = UUIDSerializer.serializer.deserialize(in, version);
        InetAddress owner = CompactEndpointSerializationHelper.deserialize(in);
        InetAddress src = CompactEndpointSerializationHelper.deserialize(in);
        InetAddress dst = CompactEndpointSerializationHelper.deserialize(in);
        String keyspace = in.readUTF();
        String cfName = in.readUTF();
        int rangesCount = in.readInt();
        List<Range<Token>> ranges = new ArrayList<>(rangesCount);
        for (int i = 0; i < rangesCount; ++i)
            ranges.add((Range<Token>) AbstractBounds.serializer.deserialize(in, version).toTokenBounds());
        // for older version, id here is not RepairSession id, but StreamingRepairTask id.
        // but since we omit id from StreamingRepairTask, we use id in RepairJobDesc instead to
        // send back id.
        RepairJobDesc desc = new RepairJobDesc(id, keyspace, cfName, null);
        SyncRequest request = new SyncRequest(owner, src, dst, ranges);
        return new StreamingRepairTask(desc, request);
    }

    public long serializedSize(StreamingRepairTask task, int version)
    {
        throw new UnsupportedOperationException("this version does not serialize StreamingRepairTask");
    }
}

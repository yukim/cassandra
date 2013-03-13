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
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Body part of SYNC_REQUEST repair message.
 * Request {@code src} node to sync data with {@code dst} node for range {@code ranges}.
 *
 * @since 2.0
 */
public class SyncRequest
{
    public static IVersionedSerializer<SyncRequest> serializer = new SyncRequestSerializer();

    public final InetAddress initiator;
    public final InetAddress src;
    public final InetAddress dst;
    public final Collection<Range<Token>> ranges;

    /**
     * Create SyncRequest so that the initiator would be the local node executing this method,
     * and {@code src} would be the local node if either of given endpoints is the local node.
     */
    public static SyncRequest create(InetAddress ep1, InetAddress ep2, Collection<Range<Token>> ranges)
    {
        InetAddress local = FBUtilities.getBroadcastAddress();
        // We can take anyone of the node as source or destination, however if one is localhost, we put at source to avoid a forwarding
        InetAddress src = ep2.equals(local) ? ep2 : ep1;
        InetAddress dst = ep2.equals(local) ? ep1 : ep2;
        return new SyncRequest(local, src, dst, ranges);
    }

    public SyncRequest(InetAddress initiator, InetAddress src, InetAddress dst, Collection<Range<Token>> ranges)
    {
        this.initiator = initiator;
        this.src = src;
        this.dst = dst;
        this.ranges = ranges;
    }

    public static class SyncRequestSerializer implements IVersionedSerializer<SyncRequest>
    {
        public void serialize(SyncRequest request, DataOutput out, int version) throws IOException
        {
            CompactEndpointSerializationHelper.serialize(request.initiator, out);
            CompactEndpointSerializationHelper.serialize(request.src, out);
            CompactEndpointSerializationHelper.serialize(request.dst, out);
            out.writeInt(request.ranges.size());
            for (Range<Token> range : request.ranges)
                AbstractBounds.serializer.serialize(range, out, version);
        }

        public SyncRequest deserialize(DataInput in, int version) throws IOException
        {
            InetAddress owner = CompactEndpointSerializationHelper.deserialize(in);
            InetAddress src = CompactEndpointSerializationHelper.deserialize(in);
            InetAddress dst = CompactEndpointSerializationHelper.deserialize(in);
            int rangesCount = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(rangesCount);
            for (int i = 0; i < rangesCount; ++i)
                ranges.add((Range<Token>) AbstractBounds.serializer.deserialize(in, version).toTokenBounds());
            return new SyncRequest(owner, src, dst, ranges);
        }

        public long serializedSize(SyncRequest request, int version)
        {
            long size = 0;
            size += 3 * CompactEndpointSerializationHelper.serializedSize(request.initiator);
            size += TypeSizes.NATIVE.sizeof(request.ranges.size());
            for (Range<Token> range : request.ranges)
                size += AbstractBounds.serializer.serializedSize(range, version);
            return size;
        }
    }
}

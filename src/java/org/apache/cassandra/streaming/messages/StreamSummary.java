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
package org.apache.cassandra.streaming.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.utils.CFPath;

/**
 * Summary of streaming.
 */
public class StreamSummary
{
    public static final ISerializer<StreamSummary> serializer = new StreamSummarySerializer();

    public final CFPath path;

    /**
     * Number of files to transfer. Can be 0 if nothing to transfer for some streaming request.
     */
    public final int files;
    public final long totalSize;

    public StreamSummary(CFPath path, int files, long totalSize)
    {
        this.path = path;
        this.files = files;
        this.totalSize = totalSize;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("StreamSummary{");
        sb.append("path=").append(path);
        sb.append(", files=").append(files);
        sb.append(", totalSize=").append(totalSize);
        sb.append('}');
        return sb.toString();
    }

    public static class StreamSummarySerializer implements ISerializer<StreamSummary>
    {
        public void serialize(StreamSummary summary, DataOutput out) throws IOException
        {
            out.writeUTF(summary.path.keyspace());
            out.writeUTF(summary.path.columnFamily());
            out.writeInt(summary.files);
            out.writeLong(summary.totalSize);
        }

        public StreamSummary deserialize(DataInput in) throws IOException
        {
            String keyspace = in.readUTF();
            String columnFamily = in.readUTF();
            int files = in.readInt();
            long totalSize = in.readLong();
            return new StreamSummary(new CFPath(keyspace, columnFamily), files, totalSize);
        }

        public long serializedSize(StreamSummary summary, TypeSizes type)
        {
            long size = type.sizeof(summary.path.keyspace());
            size += type.sizeof(summary.path.columnFamily());
            size += type.sizeof(summary.files);
            size += type.sizeof(summary.totalSize);
            return size;
        }
    }
}

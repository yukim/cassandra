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
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.compress.CompressionInfo;
import org.apache.cassandra.utils.CFPath;
import org.apache.cassandra.utils.Pair;

/**
 * StreamingFileHeader is appended before sending actual data to describe what it's sending.
 */
public class FileMessageHeader
{
    public static ISerializer<FileMessageHeader> serializer = new FileMessageHeaderSerializer();

    public final CFPath path;
    public final int sequenceNumber;
    public final long estimatedKeys;
    public final List<Pair<Long, Long>> sections;
    public final CompressionInfo compressionInfo;

    public FileMessageHeader(CFPath path, int sequenceNumber, long estimatedKeys, List<Pair<Long, Long>> sections, CompressionInfo compressionInfo)
    {
        this.path = path;
        this.sequenceNumber = sequenceNumber;
        this.estimatedKeys = estimatedKeys;
        this.sections = sections;
        this.compressionInfo = compressionInfo;
    }

    /**
     * @return total file size to transfer in bytes
     */
    public long size()
    {
        long size = 0;
        if (compressionInfo != null)
        {
            // calculate total length of transferring chunks
            for (CompressionMetadata.Chunk chunk : compressionInfo.chunks)
                size += chunk.length + 4; // 4 bytes for CRC
        }
        else
        {
            for (Pair<Long, Long> section : sections)
                size += section.right - section.left;
        }
        return size;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileMessageHeader that = (FileMessageHeader) o;
        return sequenceNumber == that.sequenceNumber && path.equals(that.path);
    }

    @Override
    public int hashCode()
    {
        int result = path.hashCode();
        result = 31 * result + sequenceNumber;
        return result;
    }

    static class FileMessageHeaderSerializer implements ISerializer<FileMessageHeader>
    {
        public void serialize(FileMessageHeader header, DataOutput out) throws IOException
        {
            out.writeUTF(header.path.keyspace());
            out.writeUTF(header.path.columnFamily());
            out.writeInt(header.sequenceNumber);
            out.writeLong(header.estimatedKeys);

            out.writeInt(header.sections.size());
            for (Pair<Long, Long> section : header.sections)
            {
                out.writeLong(section.left);
                out.writeLong(section.right);
            }
            CompressionInfo.serializer.serialize(header.compressionInfo, out, MessagingService.current_version);
        }

        public FileMessageHeader deserialize(DataInput in) throws IOException
        {
            String keyspace = in.readUTF();
            String columnFamily = in.readUTF();
            int sequenceNumber = in.readInt();
            long estimatedKeys = in.readLong();
            int count = in.readInt();
            List<Pair<Long, Long>> sections = new ArrayList<>(count);
            for (int k = 0; k < count; k++)
                sections.add(Pair.create(in.readLong(), in.readLong()));
            CompressionInfo compressionInfo = CompressionInfo.serializer.deserialize(in, MessagingService.current_version);

            return new FileMessageHeader(new CFPath(keyspace, columnFamily), sequenceNumber, estimatedKeys, sections, compressionInfo);
        }

        public long serializedSize(FileMessageHeader header, TypeSizes type)
        {
            return 0;
        }
    }
}
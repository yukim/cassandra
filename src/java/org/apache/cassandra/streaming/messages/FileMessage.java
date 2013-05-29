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

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.streaming.compress.CompressionInfo;
import org.apache.cassandra.utils.CFPath;
import org.apache.cassandra.utils.Pair;

/**
 * FileMessage is used to transfer/receive the part(or whole) of a SSTable data file.
 *
 * Streaming or receiving file is done lazily.
 *
 * The message consists of the following:
 */
public class FileMessage extends StreamMessage
{
    public static FileMessage from(ReadableByteChannel in) throws IOException
    {
        DataInputStream input = new DataInputStream(Channels.newInputStream(in));
        return new FileMessage(FileMessageHeader.serializer.deserialize(input));
    }

    public final FileMessageHeader header;
    public final SSTableReader sstableToTransfer;

    public FileMessage(SSTableReader sstable, int sequenceNumber, long estimatedKeys, List<Pair<Long, Long>> sections)
    {
        super(Type.FILE);
        this.sstableToTransfer = sstable;

        CompressionInfo compressionInfo = null;
        if (sstable.compression)
        {
            CompressionMetadata meta = sstable.getCompressionMetadata();
            compressionInfo = new CompressionInfo(meta.getChunksForSections(sections), meta.parameters);
        }
        CFPath path = new CFPath(sstable.descriptor.ksname, sstable.descriptor.cfname);
        this.header = new FileMessageHeader(path, sequenceNumber, estimatedKeys, sections, compressionInfo);
    }

    FileMessage(FileMessageHeader header)
    {
        super(Type.FILE);
        this.header = header;

        this.sstableToTransfer = null;
    }

    @Override
    protected void writeMessage(WritableByteChannel out) throws IOException
    {
        DataOutput output = new DataOutputStream(Channels.newOutputStream(out));
        FileMessageHeader.serializer.serialize(header, output);
        /*
        StreamingMetrics.totalOutgoingBytes.inc(totalBytesTransferred);
        metrics.outgoingBytes.inc(totalBytesTransferred);
        */
    }
}

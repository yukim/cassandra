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
package org.apache.cassandra.streaming;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;

import com.google.common.base.Throwables;
import com.ning.compress.lzf.LZFInputStream;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.streaming.messages.StreamMessageListener;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.BytesReadTracker;
import org.apache.cassandra.utils.CFPath;
import org.apache.cassandra.utils.Pair;

/**
 * StreamReader reads from stream and writes to SSTable.
 */
public class StreamReader extends ProgressSupport
{
    protected final CFPath path;
    protected final long estimatedKeys;
    protected final Collection<Pair<Long, Long>> sections;

    protected Descriptor desc;

    public StreamReader(FileMessageHeader header, StreamMessageListener listener)
    {
        super(ProgressSupport.IN, listener);
        this.path = header.path;
        this.estimatedKeys = header.estimatedKeys;
        this.sections = header.sections;
    }

    /**
     * @param channel where this reads data from
     * @return SSTable transferred
     * @throws IOException if reading the remote sstable fails. Will throw an RTE if local write fails.
     */
    public SSTableReader read(ReadableByteChannel channel) throws IOException
    {
        long size = totalSize();

        ColumnFamilyStore cfs = Table.open(path.keyspace()).getColumnFamilyStore(path.columnFamily());
        Directories.DataDirectory localDir = cfs.directories.getLocationCapableOfSize(size);
        if (localDir == null)
            throw new IOException("Insufficient disk space to store " + size + " bytes");
        desc = Descriptor.fromFilename(cfs.getTempSSTablePath(cfs.directories.getLocationForDisk(localDir)));

        SSTableWriter writer = new SSTableWriter(desc.filenameFor(Component.DATA), estimatedKeys);
        try
        {
            DataInputStream dis = new DataInputStream(new LZFInputStream(Channels.newInputStream(channel)));
            BytesReadTracker in = new BytesReadTracker(dis);

            while (in.getBytesRead() < size)
            {
                writeRow(writer, in, cfs);
                maybeFireProgressEvent(desc, in.getBytesRead());
            }
            return writer.closeAndOpenReader();
        }
        catch (Throwable e)
        {
            writer.abort();
            if (e instanceof IOException)
                throw (IOException) e;
            else
                throw Throwables.propagate(e);
        }
    }

    @Override
    protected long totalSize()
    {
        long size = 0;
        for (Pair<Long, Long> section : sections)
            size += section.right - section.left;
        return size;
    }

    protected void writeRow(SSTableWriter writer, DataInput in, ColumnFamilyStore cfs) throws IOException
    {
        DecoratedKey key = StorageService.getPartitioner().decorateKey(ByteBufferUtil.readWithShortLength(in));
        writer.appendFromStream(key, cfs.metadata, in);
        cfs.invalidateCachedRow(key);
    }
}

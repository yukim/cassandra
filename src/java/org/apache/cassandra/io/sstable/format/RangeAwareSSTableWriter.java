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
package org.apache.cassandra.io.sstable.format;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.service.StorageService;

public class RangeAwareSSTableWriter
{
    private final List<RowPosition> boundaries;
    private final Directories.DataDirectory[] directories;
    private final int sstableLevel;
    private final long estimatedKeys;
    private final long repairedAt;
    private final SSTableFormat.Type format;
    private int currentIndex = -1;
    public final ColumnFamilyStore cfs;
    private final List<SSTableWriter> finishedWriters = new ArrayList<>();
    private SSTableWriter currentWriter = null;

    public RangeAwareSSTableWriter(ColumnFamilyStore cfs, long estimatedKeys, long repairedAt, SSTableFormat.Type format, int sstableLevel, long totalSize) throws IOException
    {
        directories = cfs.directories.getWriteableLocations();
        this.sstableLevel = sstableLevel;
        this.cfs = cfs;
        this.estimatedKeys = estimatedKeys / directories.length;
        this.repairedAt = repairedAt;
        this.format = format;
        boundaries = StorageService.getDiskBoundaries(cfs);
        if (boundaries == null)
        {
            Directories.DataDirectory localDir = cfs.directories.getWriteableLocation(totalSize);
            if (localDir == null)
                throw new IOException("Insufficient disk space to store " + totalSize + " bytes");
            Descriptor desc = Descriptor.fromFilename(cfs.getTempSSTablePath(cfs.directories.getLocationForDisk(localDir), format));
            currentWriter = SSTableWriter.create(desc, estimatedKeys, repairedAt, sstableLevel);
        }
    }

    private void verifyWriter(DecoratedKey key)
    {
        if (boundaries == null)
            return;
        boolean switched = false;
        while (currentIndex < 0 || key.compareTo(boundaries.get(currentIndex)) > 0)
        {
            switched = true;
            currentIndex++;
        }

        if (switched)
        {
            if (currentWriter != null)
                finishedWriters.add(currentWriter);
            Directories.DataDirectory dir = directories[currentIndex];
            String file = cfs.getTempSSTablePath(cfs.directories.getLocationForDisk(dir));
            Descriptor desc = Descriptor.fromFilename(file, format);
            currentWriter = SSTableWriter.create(desc, estimatedKeys, repairedAt, sstableLevel);
        }
    }

    public long appendFromStream(DecoratedKey key, CFMetaData metadata, DataInput in, Version version) throws IOException
    {
        verifyWriter(key);
        return currentWriter.appendFromStream(key, metadata, in, version);
    }

    public List<SSTableReader> close()
    {
        if (currentWriter != null)
            finishedWriters.add(currentWriter);

        List<SSTableReader> sstableReaders = new ArrayList<>();
        for (SSTableWriter writer : finishedWriters)
        {
            if (writer.getFilePointer() > 0)
                sstableReaders.add(writer.finish(true));
            else
                writer.abort();
        }
        return sstableReaders;
    }

    public void abort()
    {
        if (currentWriter != null)
            finishedWriters.add(currentWriter);

        for (SSTableWriter writer : finishedWriters)
            writer.abort();
    }

    public String getFilename()
    {
        if (currentWriter != null)
            return currentWriter.getFilename();
        return "null";
    }

    public List<SSTableWriter> getWriters()
    {
        return finishedWriters;
    }

    public SSTableWriter currentWriter()
    {
        return currentWriter;
    }
}

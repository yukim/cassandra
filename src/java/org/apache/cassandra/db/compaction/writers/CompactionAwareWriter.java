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

package org.apache.cassandra.db.compaction.writers;

import java.io.File;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.concurrent.Transactional;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.service.StorageService;


/**
 * Class that abstracts away the actual writing of files to make it possible to use CompactionTask for more
 * use cases.
 */
public abstract class CompactionAwareWriter extends Transactional.AbstractTransactional implements Transactional
{
    protected static final Logger logger = LoggerFactory.getLogger(DefaultCompactionWriter.class);

    protected final ColumnFamilyStore cfs;
    protected final Set<SSTableReader> nonExpiredSSTables;
    protected final long estimatedTotalKeys;
    protected final long maxAge;
    protected final long minRepairedAt;
    protected final SSTableRewriter sstableWriter;
    private final Directories.DataDirectory[] locations;
    private final List<RowPosition> diskBoundaries;
    private int locationIndex;
    private long repairedAt;

    public CompactionAwareWriter(ColumnFamilyStore cfs, Set<SSTableReader> allSSTables, Set<SSTableReader> nonExpiredSSTables, boolean offline)
    {
        this.cfs = cfs;
        this.nonExpiredSSTables = nonExpiredSSTables;
        this.estimatedTotalKeys = SSTableReader.getApproximateKeyCount(nonExpiredSSTables);
        this.maxAge = CompactionTask.getMaxDataAge(nonExpiredSSTables);
        this.minRepairedAt = CompactionTask.getMinRepairedAt(nonExpiredSSTables);
        this.sstableWriter = new SSTableRewriter(cfs, allSSTables, maxAge, offline);
        this.locations = cfs.directories.getWriteableLocations();
        this.diskBoundaries = StorageService.getDiskBoundaries(cfs);
        this.locationIndex = -1;
    }

    /**
     * Writes a row in an implementation specific way
     * @param row the row to append
     * @return true if the row was written, false otherwise
     */
    public final boolean append(AbstractCompactedRow row)
    {
        maybeSwitchWriter(row.key);
        return realAppend(row);
    }

    public final boolean tryAppend(AbstractCompactedRow row)
    {
        maybeSwitchWriter(row.key);
        return realTryAppend(row);
    }

    protected void maybeSwitchWriter(DecoratedKey key)
    {
        if (diskBoundaries == null)
        {
            if (locationIndex < 0)
            {
                Directories.DataDirectory panicLocation = getWriteDirectory(nonExpiredSSTables, cfs.getExpectedCompactedFileSize(nonExpiredSSTables, OperationType.UNKNOWN));
                assert panicLocation != null;
                switchCompactionLocation(panicLocation);
                locationIndex = 0;
            }
            return;
        }

        if (locationIndex > -1 && key.compareTo(diskBoundaries.get(locationIndex)) < 0)
            return;

        int prevIdx = locationIndex;
        while (locationIndex == -1 || key.compareTo(diskBoundaries.get(locationIndex)) > 0)
            locationIndex++;
        if (prevIdx >= 0)
            logger.debug("Switching write location from {} to {}", locations[prevIdx], locations[locationIndex]);
        switchCompactionLocation(locations[locationIndex]);
    }

    protected abstract boolean realAppend(AbstractCompactedRow row);

    protected boolean realTryAppend(AbstractCompactedRow row)
    {
        throw new UnsupportedOperationException("Can not do tryAppend with this CompactionAwareWriter");
    }

    public abstract void switchCompactionLocation(Directories.DataDirectory location);

    @Override
    protected Throwable doAbort(Throwable accumulate)
    {
        return sstableWriter.abort(accumulate);
    }

    @Override
    protected Throwable doCleanup(Throwable accumulate)
    {
        return accumulate;
    }

    @Override
    protected Throwable doCommit(Throwable accumulate)
    {
        return sstableWriter.commit(accumulate);
    }

    @Override
    protected void doPrepare()
    {
        sstableWriter.prepareToCommit();
    }

    /**
     * we are done, return the finished sstables so that the caller can mark the old ones as compacted
     * @return all the written sstables sstables
     */
    @Override
    public List<SSTableReader> finish()
    {
        super.finish();
        return sstableWriter.finished();
    }

    public abstract List<SSTableReader> finish(long repairedAt);

    /**
     * estimated number of keys we should write
     */
    public long estimatedKeys()
    {
        return estimatedTotalKeys;
    }

    /**
     * The directories we can write to
     */
    public Directories getDirectories()
    {
        return cfs.directories;
    }

    /**
     * Return a directory where we can expect expectedWriteSize to fit.
     *
     * @param sstables the sstables to compact
     * @return
     */
    public Directories.DataDirectory getWriteDirectory(Iterable<SSTableReader> sstables, long estimatedWriteSize)
    {
        File directory = null;
        for (SSTableReader sstable : sstables)
        {
            if (directory == null)
                directory = sstable.descriptor.directory;
            if (!directory.equals(sstable.descriptor.directory))
                logger.debug("All sstables not from the same disk - putting results in {}", directory);
        }
        Directories.DataDirectory d = cfs.directories.getDataDirectoryForFile(directory);
        if (d != null)
        {
            logger.debug("putting compaction results in {}", directory);
            return d;
        }
        return cfs.directories.getWriteableLocation(estimatedWriteSize);
    }

    public CompactionAwareWriter setRepairedAt(long repairedAt)
    {
        this.sstableWriter.setRepairedAt(repairedAt);
        return this;
    }
}
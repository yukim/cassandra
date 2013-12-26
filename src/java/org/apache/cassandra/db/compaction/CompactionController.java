/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db.compaction;

import java.util.*;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Throttle;
import org.apache.cassandra.utils.IntervalTree.Interval;
import org.apache.cassandra.utils.IntervalTree.IntervalTree;

/**
 * Manage compaction options.
 */
public class CompactionController
{
    private static Logger logger = LoggerFactory.getLogger(CompactionController.class);

    public final ColumnFamilyStore cfs;
    private final boolean deserializeRequired;
    private final IntervalTree<SSTableReader> overlappingTree;
    private final Map<Range<Token>, Integer> lastSuccessfulRepair = new HashMap<Range<Token>, Integer>();

    private final int gcBefore;
    private final boolean keyExistenceIsExpensive;
    public final int mergeShardBefore;
    private final Throttle throttle = new Throttle("Cassandra_Throttle", new Throttle.ThroughputFunction()
    {
        /** @return Instantaneous throughput target in bytes per millisecond. */
        public int targetThroughput()
        {
            if (DatabaseDescriptor.getCompactionThroughputMbPerSec() < 1 || StorageService.instance.isBootstrapMode())
                // throttling disabled
                return 0;
            // total throughput
            int totalBytesPerMS = DatabaseDescriptor.getCompactionThroughputMbPerSec() * 1024 * 1024 / 1000;
            // per stream throughput (target bytes per MS)
            return totalBytesPerMS / Math.max(1, CompactionManager.instance.getActiveCompactions());
        }
    });

    public CompactionController(ColumnFamilyStore cfs, Collection<SSTableReader> sstables, int gcBefore, boolean forceDeserialize)
    {
        this(cfs,
             gcBefore,
             forceDeserialize || !allLatestVersion(sstables),
             DataTracker.buildIntervalTree(cfs.getOverlappingSSTables(sstables)),
             cfs.getCompactionStrategy().isKeyExistenceExpensive(ImmutableSet.copyOf(sstables)));
    }

    protected CompactionController(ColumnFamilyStore cfs,
                                   int gcBefore,
                                   boolean deserializeRequired,
                                   IntervalTree<SSTableReader> overlappingTree,
                                   boolean keyExistenceIsExpensive)
    {
        assert cfs != null;
        this.cfs = cfs;
        this.gcBefore = gcBefore;
        // If we merge an old NodeId id, we must make sure that no further increment for that id are in an active memtable.
        // For that, we must make sure that this id was renewed before the creation of the oldest unflushed memtable. We
        // add 5 minutes to be sure we're on the safe side in terms of thread safety (though we should be fine in our
        // current 'stop all write during memtable switch' situation).
        this.mergeShardBefore = (int) ((cfs.oldestUnflushedMemtable() + 5 * 3600) / 1000);
        this.deserializeRequired = deserializeRequired;
        this.overlappingTree = overlappingTree;
        this.keyExistenceIsExpensive = keyExistenceIsExpensive;
    }

    public String getKeyspace()
    {
        return cfs.table.name;
    }

    public String getColumnFamily()
    {
        return cfs.columnFamily;
    }

    public void addLastSuccessfulRepair(Map<Range<Token>, Integer> lastSuccessfulRepair)
    {
        this.lastSuccessfulRepair.putAll(lastSuccessfulRepair);
    }

    /**
     * Returns timestamp in seconds that, compaction can purge tombstones *before* this timestamp.
     * If given Token {@code t} is found in the range of last successful repair, this returns
     * Min("Actual GC timestamp", "Last successful repair timestamp for t").
     * If Token {@code t} is not found in any ranges of last successful repair, then this returns
     * Integer.MIN_VALUE so that not repaired partition won't drop tombstones.
     *
     * @param t Token of partition key
     * @return timestamp for gc
     */
    public int gcBefore(Token t)
    {
        // special case system keyspace since it won't get repaired
        if (Table.SYSTEM_TABLE.equals(cfs.metadata.ksName))
            return gcBefore;

        assert t != null;
        int lastRepairTime = Integer.MIN_VALUE;
        for (Range<Token> range : lastSuccessfulRepair.keySet())
        {
            if (range.contains(t))
            {
                lastRepairTime = lastSuccessfulRepair.get(range);
                break;
            }
        }

        return Math.min(lastRepairTime, gcBefore);
    }

    /**
     * @return true if it's okay to drop tombstones for the given row, i.e., if we know all the verisons of the row
     * are included in the compaction set
     */
    public boolean shouldPurge(DecoratedKey key)
    {
        List<SSTableReader> filteredSSTables = overlappingTree.search(new Interval(key, key));
        for (SSTableReader sstable : filteredSSTables)
        {
            if (sstable.getBloomFilter().isPresent(key.key))
                return false;
        }
        return true;
    }

    private static boolean allLatestVersion(Iterable<SSTableReader> sstables)
    {
        for (SSTableReader sstable : sstables)
            if (!sstable.descriptor.isLatestVersion)
                return false;
        return true;
    }

    public void invalidateCachedRow(DecoratedKey key)
    {
        cfs.invalidateCachedRow(key);
    }

    public void removeDeletedInCache(DecoratedKey key)
    {
        // For the copying cache, we'd need to re-serialize the updated cachedRow, which would be racy
        // vs other updates.  We'll just ignore it instead, since the next update to this row will invalidate it
        // anyway, so the odds of a "tombstones consuming memory indefinitely" problem are minimal.
        // See https://issues.apache.org/jira/browse/CASSANDRA-3921 for more discussion.
        if (CacheService.instance.rowCache.isPutCopying())
            return;

        ColumnFamily cachedRow = cfs.getRawCachedRow(key);
        if (cachedRow != null)
            ColumnFamilyStore.removeDeleted(cachedRow, gcBefore(key.getToken()));
    }

    /**
     * @return an AbstractCompactedRow implementation to write the merged rows in question.
     *
     * If there is a single source row, the data is from a current-version sstable, we don't
     * need to purge and we aren't forcing deserialization for scrub, write it unchanged.
     * Otherwise, we deserialize, purge tombstones, and reserialize in the latest version.
     */
    public AbstractCompactedRow getCompactedRow(List<SSTableIdentityIterator> rows)
    {
        long rowSize = 0;
        for (SSTableIdentityIterator row : rows)
            rowSize += row.dataSize;

        // in-memory echoedrow is only enabled if we think checking for the key's existence in the other sstables,
        // is going to be less expensive than simply de/serializing the row again
        if (rows.size() == 1 && !deserializeRequired
            && (rowSize > DatabaseDescriptor.getInMemoryCompactionLimit() || !keyExistenceIsExpensive)
            && !shouldPurge(rows.get(0).getKey()))
        {
            return new EchoedRow(rows.get(0));
        }

        if (rowSize > DatabaseDescriptor.getInMemoryCompactionLimit())
        {
            String keyString = cfs.metadata.getKeyValidator().getString(rows.get(0).getKey().key);
            logger.info(String.format("Compacting large row %s/%s:%s (%d bytes) incrementally",
                                      cfs.table.name, cfs.columnFamily, keyString, rowSize));
            return new LazilyCompactedRow(this, rows);
        }
        return new PrecompactedRow(this, rows);
    }

    /** convenience method for single-sstable compactions */
    public AbstractCompactedRow getCompactedRow(SSTableIdentityIterator row)
    {
        return getCompactedRow(Collections.singletonList(row));
    }
    
    public void mayThrottle(long currentBytes)
    {
        throttle.throttle(currentBytes);
    }
}

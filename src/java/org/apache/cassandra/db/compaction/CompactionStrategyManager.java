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
package org.apache.cassandra.db.compaction;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableDeletingNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.service.StorageService;

/**
 * Manages the compaction strategies.
 *
 * Currently has two instances of actual compaction strategies per data directory - one for repaired data and one for
 * unrepaired data. This is done to be able to totally separate the different sets of sstables.
 */

public class CompactionStrategyManager implements INotificationConsumer
{
    protected static final String COMPACTION_ENABLED = "enabled";
    private static final Logger logger = LoggerFactory.getLogger(CompactionStrategyManager.class);
    private final ColumnFamilyStore cfs;
    private volatile List<AbstractCompactionStrategy> repaired = new ArrayList<>();
    private volatile List<AbstractCompactionStrategy> unrepaired = new ArrayList<>();
    private volatile boolean enabled = true;
    public boolean isActive = true;
    private Directories.DataDirectory[] locations;


    public CompactionStrategyManager(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        reload(cfs.metadata);
        cfs.getDataTracker().subscribe(this);
        locations = cfs.directories.getWriteableLocations();
        logger.debug("{} subscribed to the data tracker.", this);
        String optionValue = cfs.metadata.compactionStrategyOptions.get(COMPACTION_ENABLED);
        enabled = optionValue == null || Boolean.parseBoolean(optionValue);
    }

    /**
     * Return the next background task
     *
     * Returns a task for the compaction strategy that needs it the most (most estimated remaining tasks)
     *
     */
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        if (!isEnabled())
            return null;

        maybeReload(cfs.metadata);

        List<AbstractCompactionStrategy> strategies = new ArrayList<>(repaired.size() + unrepaired.size());
        strategies.addAll(repaired);
        strategies.addAll(unrepaired);
        Collections.sort(strategies, new Comparator<AbstractCompactionStrategy>()
        {
            @Override
            public int compare(AbstractCompactionStrategy o1, AbstractCompactionStrategy o2)
            {
                return Ints.compare(o2.getEstimatedRemainingTasks(), o1.getEstimatedRemainingTasks());
            }
        });
        for (AbstractCompactionStrategy strategy : strategies)
        {
            AbstractCompactionTask task = strategy.getNextBackgroundTask(gcBefore);
            if (task != null)
                return task;
        }
        return null;
    }

    /**
     * Disable compaction - used with nodetool disableautocompaction for example
     */
    public void disable()
    {
        enabled = false;
    }

    /**
     * re-enable disabled compaction.
     */
    public void enable()
    {
        enabled = true;
    }

    public boolean isEnabled()
    {
        return enabled && isActive;
    }

    public void resume()
    {
        isActive = true;
    }

    /**
     * pause compaction while we cancel all ongoing compactions
     *
     * Separate call from enable/disable to not have to save the enabled-state externally
      */
    public void pause()
    {
        isActive = false;
    }


    private void startup()
    {
        for (SSTableReader sstable : cfs.getSSTables())
        {
            if (sstable.openReason != SSTableReader.OpenReason.EARLY)
                getCompactionStrategyFor(sstable).addSSTable(sstable);
        }
        for (AbstractCompactionStrategy strategy : repaired)
            strategy.startup();
        for (AbstractCompactionStrategy strategy : unrepaired)
            strategy.startup();
    }

    /**
     * return the compaction strategy for the given sstable
     *
     * returns differently based on the repaired status and which vnode the compaction strategy belongs to
     * @param sstable
     * @return
     */
    private AbstractCompactionStrategy getCompactionStrategyFor(SSTableReader sstable)
    {
        int index = getCompactionStrategyIndex(cfs, sstable.descriptor);
        if (sstable.isRepaired())
            return repaired.get(index);
        else
            return unrepaired.get(index);
    }

    public static int getCompactionStrategyIndex(ColumnFamilyStore cfs, Descriptor descriptor)
    {
        if (!cfs.partitioner.supportsSplitting())
            return 0;

        Directories.DataDirectory[] directories = cfs.directories.getWriteableLocations();
        for (int i = 0; i < directories.length; i++)
        {
            Directories.DataDirectory directory = directories[i];
            if (descriptor.directory.getPath().startsWith(directory.location.getPath()))
                return i;
        }
        logger.debug("Could not find directory for {} - putting it in {} instead", descriptor, directories[0].location);
        return 0;
    }

    public void shutdown()
    {
        isActive = false;
        for (AbstractCompactionStrategy strategy : repaired)
            strategy.shutdown();
        for (AbstractCompactionStrategy strategy : unrepaired)
            strategy.shutdown();

    }


    public synchronized void maybeReload(CFMetaData metadata)
    {
        if (repaired != null && repaired.get(0).getClass().equals(metadata.compactionStrategyClass)
                && unrepaired != null && unrepaired.get(0).getClass().equals(metadata.compactionStrategyClass)
                && repaired.get(0).options.equals(metadata.compactionStrategyOptions) // todo: assumes all have the same options
                && unrepaired.get(0).options.equals(metadata.compactionStrategyOptions)
                && Arrays.equals(locations, cfs.directories.getWriteableLocations())) // any drives broken?
            return;

        reload(metadata);
    }

    /**
     * Reload the compaction strategies
     *
     * Called after changing configuration and at startup.
     * @param metadata
     */
    public synchronized void reload(CFMetaData metadata)
    {
        for (AbstractCompactionStrategy strategy : repaired)
            strategy.shutdown();
        for (AbstractCompactionStrategy strategy : unrepaired)
            strategy.shutdown();
        repaired.clear();
        unrepaired.clear();
        if (cfs.partitioner.supportsSplitting())
        {
            locations = cfs.directories.getWriteableLocations();
            for (int i = 0; i < locations.length; i++)
            {
                repaired.add(metadata.createCompactionStrategyInstance(cfs));
                unrepaired.add(metadata.createCompactionStrategyInstance(cfs));
            }
        }
        else
        {
            repaired.add(metadata.createCompactionStrategyInstance(cfs));
            unrepaired.add(metadata.createCompactionStrategyInstance(cfs));
        }
        startup();
    }

    public void replaceFlushed(Memtable memtable, List<SSTableReader> sstables)
    {
        cfs.getDataTracker().replaceFlushed(memtable, sstables);
        if (sstables != null)
            CompactionManager.instance.submitBackground(cfs);
    }

    public List<SSTableReader> filterSSTablesForReads(List<SSTableReader> sstables)
    {
        // todo: union of filtered sstables or intersection?
        return unrepaired.get(0).filterSSTablesForReads(repaired.get(0).filterSSTablesForReads(sstables));
    }

    public int getUnleveledSSTables()
    {
        if (repaired.get(0) instanceof LeveledCompactionStrategy && unrepaired.get(0) instanceof LeveledCompactionStrategy)
        {
            int count = 0;
            for (AbstractCompactionStrategy strategy : repaired)
                count += ((LeveledCompactionStrategy)strategy).getLevelSize(0);
            for (AbstractCompactionStrategy strategy : unrepaired)
                count += ((LeveledCompactionStrategy)strategy).getLevelSize(0);
            return count;
        }
        return 0;
    }

    public synchronized int[] getSSTableCountPerLevel()
    {
        if (repaired.get(0) instanceof LeveledCompactionStrategy && unrepaired.get(0) instanceof LeveledCompactionStrategy)
        {
            int [] res = new int[15];
            for (AbstractCompactionStrategy strategy : repaired)
            {
                int[] repairedCountPerLevel = ((LeveledCompactionStrategy) strategy).getAllLevelSize();
                res = sumArrays(res, repairedCountPerLevel);
            }
            for (AbstractCompactionStrategy strategy : unrepaired)
            {
                int[] unrepairedCountPerLevel = ((LeveledCompactionStrategy) strategy).getAllLevelSize();
                res = sumArrays(res, unrepairedCountPerLevel);
            }

            return res;
        }
        return null;
    }

    public static int[] sumArrays(int[] a, int[] b)
    {
        int[] res = new int[Math.max(a.length, b.length)];
        for (int i = 0; i < res.length; i++)
        {
            if (i < a.length && i < b.length)
                res[i] = a[i] + b[i];
            else if (i < a.length)
                res[i] = a[i];
            else
                res[i] = b[i];
        }
        return res;
    }

    public boolean shouldDefragment()
    {
        assert repaired.get(0).getClass().equals(unrepaired.get(0).getClass());
        return repaired.get(0).shouldDefragment();
    }


    public synchronized void handleNotification(INotification notification, Object sender)
    {
        if (notification instanceof SSTableAddedNotification)
        {
            SSTableAddedNotification flushedNotification = (SSTableAddedNotification) notification;
            getCompactionStrategyFor(flushedNotification.added).addSSTable(flushedNotification.added);
        }
        else if (notification instanceof SSTableListChangedNotification)
        {
            // a bit of gymnastics to be able to replace sstables in compaction strategies
            // we use this to know that a compaction finished and where to start the next compaction in LCS
            SSTableListChangedNotification listChangedNotification = (SSTableListChangedNotification) notification;

            Directories.DataDirectory [] locations = cfs.directories.getWriteableLocations();
            int locationSize = cfs.partitioner.supportsSplitting() ? locations.length : 1;

            List<Set<SSTableReader>> repairedRemoved = new ArrayList<>(locationSize);
            List<Set<SSTableReader>> repairedAdded = new ArrayList<>(locationSize);
            List<Set<SSTableReader>> unrepairedRemoved = new ArrayList<>(locationSize);
            List<Set<SSTableReader>> unrepairedAdded = new ArrayList<>(locationSize);

            for (int i = 0; i < locationSize; i++)
            {
                repairedRemoved.add(new HashSet<>());
                repairedAdded.add(new HashSet<>());
                unrepairedRemoved.add(new HashSet<>());
                unrepairedAdded.add(new HashSet<>());
            }

            for (SSTableReader sstable : listChangedNotification.removed)
            {
                int i = getCompactionStrategyIndex(cfs, sstable.descriptor);
                if (sstable.isRepaired())
                    repairedRemoved.get(i).add(sstable);
                else
                    unrepairedRemoved.get(i).add(sstable);
            }
            for (SSTableReader sstable : listChangedNotification.added)
            {
                int i = getCompactionStrategyIndex(cfs, sstable.descriptor);
                if (sstable.isRepaired())
                    repairedAdded.get(i).add(sstable);
                else
                    unrepairedAdded.get(i).add(sstable);
            }

            for (int i = 0; i < locationSize; i++)
            {
                if (!repairedRemoved.get(i).isEmpty())
                    repaired.get(i).replaceSSTables(repairedRemoved.get(i), repairedAdded.get(i));
                else
                {
                    for (SSTableReader sstable : repairedAdded.get(i))
                        repaired.get(i).addSSTable(sstable);
                }
                if (!unrepairedRemoved.get(i).isEmpty())
                    unrepaired.get(i).replaceSSTables(unrepairedRemoved.get(i), unrepairedAdded.get(i));
                else
                {
                    for (SSTableReader sstable : unrepairedAdded.get(i))
                        unrepaired.get(i).addSSTable(sstable);
                }
            }
        }
        else if (notification instanceof SSTableRepairStatusChanged)
        {
            for (SSTableReader sstable : ((SSTableRepairStatusChanged) notification).sstable)
            {
                int index = getCompactionStrategyIndex(cfs, sstable.descriptor);
                if (sstable.isRepaired())
                {
                    unrepaired.get(index).removeSSTable(sstable);
                    repaired.get(index).addSSTable(sstable);
                }
                else
                {
                    repaired.get(index).removeSSTable(sstable);
                    unrepaired.get(index).addSSTable(sstable);
                }
            }
        }
        else if (notification instanceof SSTableDeletingNotification)
        {
            SSTableReader sstable = ((SSTableDeletingNotification) notification).deleting;
            getCompactionStrategyFor(sstable).removeSSTable(sstable);
        }
    }

    /**
     * Create ISSTableScanners from the given sstables
     *
     * Delegates the call to the compaction strategies to allow LCS to create a scanner
     * @param sstables
     * @param range
     * @return
     */
    public synchronized AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables, Range<Token> range)
    {
        assert repaired.size() == unrepaired.size();
        List<Set<SSTableReader>> repairedSSTables = new ArrayList<>();
        List<Set<SSTableReader>> unrepairedSSTables = new ArrayList<>();

        for (int i = 0; i < repaired.size(); i++)
        {
            repairedSSTables.add(new HashSet<>());
            unrepairedSSTables.add(new HashSet<>());
        }

        for (SSTableReader sstable : sstables)
        {
            if (sstable.isRepaired())
                repairedSSTables.get(getCompactionStrategyIndex(cfs, sstable.descriptor)).add(sstable);
            else
                unrepairedSSTables.get(getCompactionStrategyIndex(cfs, sstable.descriptor)).add(sstable);
        }

        List<ISSTableScanner> scanners = new ArrayList<>();

        for (int i = 0; i < repaired.size(); i++)
        {
            if (!repairedSSTables.get(i).isEmpty())
                scanners.addAll(repaired.get(i).getScanners(repairedSSTables.get(i), range).scanners);
            if (!unrepairedSSTables.isEmpty())
                scanners.addAll(unrepaired.get(i).getScanners(unrepairedSSTables.get(i), range).scanners);
        }

        return new AbstractCompactionStrategy.ScannerList(scanners);
    }

    public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup)
    {
        return unrepaired.get(0).groupSSTablesForAntiCompaction(sstablesToGroup);
    }

    public synchronized AbstractCompactionStrategy.ScannerList getScanners(Collection<SSTableReader> sstables)
    {
        return getScanners(sstables, null);
    }

    public long getMaxSSTableBytes()
    {
        return unrepaired.get(0).getMaxSSTableBytes();
    }

    public AbstractCompactionTask getCompactionTask(Set<SSTableReader> input, int gcBefore, long maxSSTableBytes)
    {
        validateForCompaction(input);
        return getCompactionStrategyFor(input.iterator().next()).getCompactionTask(input, gcBefore, maxSSTableBytes);
    }

    private void validateForCompaction(Iterable<SSTableReader> input)
    {
        SSTableReader firstSSTable = Iterables.getFirst(input, null);
        assert firstSSTable != null;
        boolean repaired = firstSSTable.isRepaired();
        int firstIndex = getCompactionStrategyIndex(cfs, firstSSTable.descriptor);
        for (SSTableReader sstable : input)
        {
            if (sstable.isRepaired() != repaired)
                throw new UnsupportedOperationException("You can't mix repaired and unrepaired data in a compaction");
            if (firstIndex != getCompactionStrategyIndex(cfs, sstable.descriptor))
                throw new UnsupportedOperationException("You can't mix sstables from different directories in a compaction");
        }
    }

    public Collection<AbstractCompactionTask> getMaximalTasks(final int gcBefore, final boolean splitOutput)
    {
        // runWithCompactionsDisabled cancels active compactions and disables them, then we are able
        // to make the repaired/unrepaired strategies mark their own sstables as compacting. Once the
        // sstables are marked the compactions are re-enabled
        return cfs.runWithCompactionsDisabled(new Callable<Collection<AbstractCompactionTask>>()
        {
            @Override
            public Collection<AbstractCompactionTask> call() throws Exception
            {
                synchronized (CompactionStrategyManager.this)
                {
                    List<AbstractCompactionTask> tasks = new ArrayList<>();
                    for (AbstractCompactionStrategy strategy : repaired)
                    {
                        Collection<AbstractCompactionTask> task = strategy.getMaximalTask(gcBefore, splitOutput);
                        if (task != null)
                            tasks.addAll(task);
                    }
                    for (AbstractCompactionStrategy strategy : unrepaired)
                    {
                        Collection<AbstractCompactionTask> task = strategy.getMaximalTask(gcBefore, splitOutput);
                        if (task != null)
                            tasks.addAll(task);
                    }
                    if (tasks.isEmpty())
                        return null;
                    return tasks;
                }
            }
        }, false);
    }

    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        validateForCompaction(sstables);
        return getCompactionStrategyFor(sstables.iterator().next()).getUserDefinedTask(sstables, gcBefore);
    }

    public int getEstimatedRemainingTasks()
    {
        int tasks = 0;
        for (AbstractCompactionStrategy strategy : repaired)
            tasks += strategy.getEstimatedRemainingTasks();
        for (AbstractCompactionStrategy strategy : unrepaired)
            tasks += strategy.getEstimatedRemainingTasks();

        return tasks;
    }

    public boolean shouldBeEnabled()
    {
        String optionValue = cfs.metadata.compactionStrategyOptions.get(COMPACTION_ENABLED);
        return optionValue == null || Boolean.parseBoolean(optionValue);
    }

    public String getName()
    {
        return unrepaired.get(0).getName();
    }

    public List<List<AbstractCompactionStrategy>> getStrategies()
    {
        return Arrays.asList(repaired, unrepaired);
    }

    public boolean validateStrategies()
    {
        List<RowPosition> boundaries = StorageService.getDiskBoundaries(cfs);
        if (boundaries == null)
            return true;
        boolean success = true;
        for (int i = 0; i < cfs.directories.getWriteableLocations().length; i++)
        {
            RowPosition first = (i == 0) ? cfs.partitioner.getMinimumToken().minKeyBound() : boundaries.get(i - 1);
            RowPosition last = boundaries.get(i);
            Iterable<SSTableReader> sstables = Iterables.concat(repaired.get(i).getSSTables(), unrepaired.get(i).getSSTables());
            for (SSTableReader sstable : sstables)
            {
                if (sstable.first.compareTo(first) < 0 || sstable.last.compareTo(last) > 0)
                {
                    success = false;
                    logger.error("In wrong compaction strategy: {}", sstable);
                }
            }
        }
        return success;
    }
}

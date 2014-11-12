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

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableRewriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;

public class VNodeAwareCompactionStrategy extends AbstractCompactionStrategy
{
    private static final String MIN_VNODE_SSTABLE_SIZE_MB = "min_vnode_sstable_size_in_mb";

    private static final Logger logger = LoggerFactory.getLogger(VNodeAwareCompactionStrategy.class);
    private final Set<SSTableReader> l0sstables = new HashSet<>();
    private final SizeTieredCompactionStrategyOptions sizeTieredOptions;
    private final List<Set<SSTableReader>> sstables = new ArrayList<>();
    private List<Token> vnodeBoundaries = null;
    private final Set<SSTableReader> unknownSSTables = new HashSet<>();
    private final long vnodeSSTableSize;
    private List<Range<Token>> localRanges;
    private int estimatedL1compactions = 0;
    private int estimatedL0compactions = 0;

    public VNodeAwareCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);

        if (options.containsKey(MIN_VNODE_SSTABLE_SIZE_MB))
            vnodeSSTableSize = Long.parseLong(options.get(MIN_VNODE_SSTABLE_SIZE_MB)) * (1L << 20);
        else
            vnodeSSTableSize = 10_000_000;
        sizeTieredOptions = new SizeTieredCompactionStrategyOptions(options);
        if (DatabaseDescriptor.getNumTokens() == 1)
            throw new ConfigurationException("Can't use VNodeAwareCompactionStrategy without running VNodes");
    }

    private void refreshVnodeBoundaries()
    {
        Collection<Range<Token>> lr = StorageService.instance.getLocalRanges(cfs.keyspace.getName());
        if (lr == null || lr.isEmpty())
        {
            logger.warn("Don't know the vnode boundaries yet, not doing any compaction.");
            return;
        }
        List<Range<Token>> oldLocalRanges = localRanges;
        localRanges = Range.sort(lr);
        if (oldLocalRanges != null && oldLocalRanges.equals(localRanges))
        {
            logger.debug("No change in local ranges");
            return;
        }

        if (vnodeBoundaries != null && !vnodeBoundaries.isEmpty())
            return;

        localRanges = Range.sort(lr);

        vnodeBoundaries = new ArrayList<>(localRanges.size() + 1);
        for (Range<Token> r : localRanges)
            vnodeBoundaries.add(r.right);
        vnodeBoundaries.add(cfs.partitioner.getMaximumToken());
        for (int i = 0; i < vnodeBoundaries.size(); i++)
            sstables.add(new HashSet<>());
        // unknown sstables are the ones added before we knew the vnode boundaries:
        for (SSTableReader sstable : unknownSSTables)
            addSSTable(sstable);

        unknownSSTables.clear();
        StringBuilder sb = new StringBuilder();
        sb.append("ks=").append(cfs.keyspace.getName()).append(" cf=").append(cfs.getColumnFamilyName()).append(": ");
        sb.append("L0=").append(l0sstables.size()).append(" ");
        for (Set<SSTableReader> s : sstables)
            sb.append(s.size());
        logger.info(sb.toString());

    }

    @Override
    public synchronized AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        refreshVnodeBoundaries();
        if (vnodeBoundaries == null || vnodeBoundaries.isEmpty())
            return null;
        // need to get both l0 and l1 before checking which to return in order to update estimatedRemainingCompactions
        Set<SSTableReader> l0candidates = getL0Candidates();
        Set<SSTableReader> l1candidates = getL1Candidates();

        if (l0candidates != null && cfs.getDataTracker().markCompacting(l0candidates))
            return new VNodeAwareCompactionTask(cfs, l0candidates, gcBefore, vnodeBoundaries, localRanges, cfs.directories.getWriteableLocations(), vnodeSSTableSize);
        if (l1candidates != null && cfs.getDataTracker().markCompacting(l1candidates))
            return new VNodeAwareCompactionTask(cfs, l1candidates, gcBefore, vnodeBoundaries, localRanges, cfs.directories.getWriteableLocations(), vnodeSSTableSize);
        return null;
    }

    private Set<SSTableReader> getL0Candidates()
    {
        Set<SSTableReader> nonCompactingL0 = Sets.difference(l0sstables, cfs.getDataTracker().getCompacting());
        CompactionCandidates interesting = getSSTablesForSTCS(nonCompactingL0);
        estimatedL0compactions = interesting.estimatedRemainingCompactions;
        if (!interesting.sstables.isEmpty())
            return nonCompactingL0;

        return null;
    }

    private Set<SSTableReader> getL1Candidates()
    {
        final Set<SSTableReader> compacting = cfs.getDataTracker().getCompacting();
        // for each vnodes sstables, get the most interesting STCS sstables to compact
        List<CompactionCandidates> transformed = sstables.stream().map((mapper) -> getSSTablesForSTCS(Sets.difference(mapper, compacting))).collect(Collectors.toList());
        estimatedL1compactions = transformed.stream().mapToInt((c) -> c.estimatedRemainingCompactions).sum();
        // sort by the biggest one;
        Optional<CompactionCandidates> candidate = transformed.stream().max((o1, o2) -> Longs.compare(SSTableReader.getTotalBytes(o1.sstables), SSTableReader.getTotalBytes(o2.sstables)));
        if (candidate.isPresent() && !candidate.get().sstables.isEmpty())
            return new HashSet<>(candidate.get().sstables);

        return null;
    }

    private CompactionCandidates getSSTablesForSTCS(Set<SSTableReader> sstables)
    {
        Iterable<SSTableReader> candidates = cfs.getDataTracker().getUncompactingSSTables(sstables);
        List<Pair<SSTableReader, Long>> pairs = SizeTieredCompactionStrategy.createSSTableAndLengthPairs(AbstractCompactionStrategy.filterSuspectSSTables(candidates));
        List<List<SSTableReader>> buckets = SizeTieredCompactionStrategy.getBuckets(pairs,
                sizeTieredOptions.bucketHigh,
                sizeTieredOptions.bucketLow,
                sizeTieredOptions.minSSTableSize);
        int minThreshold = cfs.getMinimumCompactionThreshold();
        int maxThreshold = cfs.getMaximumCompactionThreshold();
        return new CompactionCandidates(SizeTieredCompactionStrategy.getEstimatedCompactionsByTasks(cfs, buckets), SizeTieredCompactionStrategy.mostInterestingBucket(buckets, minThreshold, maxThreshold));
    }

    private static class CompactionCandidates
    {
        private final int estimatedRemainingCompactions;
        private final List<SSTableReader> sstables;
        public CompactionCandidates(int estimatedRemainingCompactions, List<SSTableReader> sstables)
        {
            this.estimatedRemainingCompactions = estimatedRemainingCompactions;
            this.sstables = sstables;
        }
    }

    @Override
    public Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput)
    {
        return null;
    }

    @Override
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        return null;
    }

    @Override
    public int getEstimatedRemainingTasks()
    {
        return estimatedL0compactions + estimatedL1compactions;
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return Integer.MAX_VALUE;
    }

    @Override
    public synchronized void addSSTable(SSTableReader added)
    {
        if (vnodeBoundaries == null || vnodeBoundaries.isEmpty())
        {
            unknownSSTables.add(added);
            return;
        }

        if (added.getSSTableLevel() == 0)
            l0sstables.add(added);
        else
            getSSTablesFor(added.first.getToken()).add(added);
    }

    @Override
    public synchronized void removeSSTable(SSTableReader sstable)
    {
        unknownSSTables.remove(sstable);
        if (sstable.getSSTableLevel() == 0)
            l0sstables.remove(sstable);
        else
            getSSTablesFor(sstable.first.getToken()).remove(sstable);
    }

    @Override
    public synchronized Iterable<SSTableReader> getSSTables()
    {
        Set<SSTableReader> sstables = new HashSet<>();
        sstables.addAll(l0sstables);
        sstables.addAll(sstables);
        sstables.addAll(unknownSSTables);
        return sstables;
    }

    private Set<SSTableReader> getSSTablesFor(Token token)
    {
        int pos = Collections.binarySearch(vnodeBoundaries, token);
        if (pos < 0)
            pos = -pos - 1;
        return sstables.get(pos);
    }

    private static class VNodeAwareCompactionTask extends CompactionTask
    {
        private final List<Token> vnodeBoundaries;
        private final List<Range<Token>> localRanges;
        private final Directories.DataDirectory[] locations;
        private final long vnodeSSTableSize;

        public VNodeAwareCompactionTask(ColumnFamilyStore cfs, Set<SSTableReader> l0candidates, int gcBefore, List<Token> vnodeBoundaries, List<Range<Token>> localRanges, Directories.DataDirectory[] locations, long vnodeSSTableSize)
        {
            super(cfs, l0candidates, gcBefore, false);
            this.vnodeBoundaries = vnodeBoundaries;
            this.localRanges = localRanges;
            this.locations = locations;
            this.vnodeSSTableSize = vnodeSSTableSize;
        }

        @Override
        public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs, Set<SSTableReader> allSSTables, Set<SSTableReader> nonExpiredSSTables)
        {
            return new VNodeAwareCompactionWriter(cfs, allSSTables, nonExpiredSSTables, vnodeBoundaries, localRanges, vnodeSSTableSize);
        }
    }

    public static class VNodeAwareCompactionWriter extends CompactionAwareWriter
    {
        private final SSTableRewriter l1writer;
        private final List<Token> vnodeBoundaries;
        private final double compactionGain;
        private final Set<SSTableReader> allSSTables;
        private final long vnodeSSTableSize;
        private SSTableRewriter activeWriter;
        private int vnodeIndex = -1;
        private Directories.DataDirectory location;

        public VNodeAwareCompactionWriter(ColumnFamilyStore cfs, Set<SSTableReader> allSSTables, Set<SSTableReader> nonExpiredSSTables, List<Token> vnodeBoundaries, List<Range<Token>> localRanges, long vnodeSSTableSize)
        {
            super(cfs, allSSTables, nonExpiredSSTables, false);

            l1writer = new SSTableRewriter(cfs, allSSTables, maxAge, false);
            this.vnodeBoundaries = vnodeBoundaries;
            compactionGain = SSTableReader.estimateCompactionGain(nonExpiredSSTables);
            this.allSSTables = allSSTables;
            this.vnodeSSTableSize = vnodeSSTableSize;
        }

        @Override
        public boolean realAppend(AbstractCompactedRow row)
        {
            maybeSwitchVNode(row.key);
            activeWriter.append(row);
            return false;
        }

        @Override
        public void switchCompactionLocation(Directories.DataDirectory location)
        {
            this.location = location;
            logger.info("Switching compaction location to {}", location);
            // note that we only switch the l0 writer if we switch compaction location
            sstableWriter.switchWriter(createWriter(location, estimatedTotalKeys, 0));
            if (l1writer.currentWriter() != null)
                l1writer.switchWriter(createWriter(location, estimatedTotalKeys, 1));
        }

        private void maybeSwitchVNode(DecoratedKey key)
        {
            if (vnodeIndex > -1 && key.getToken().compareTo(vnodeBoundaries.get(vnodeIndex)) < 0)
                return;

            while (vnodeIndex == -1 || key.getToken().compareTo(vnodeBoundaries.get(vnodeIndex)) >= 0)
                vnodeIndex++;

            Token vnodeStart = vnodeIndex == 0 ? cfs.partitioner.getMinimumToken() : vnodeBoundaries.get(vnodeIndex - 1);
            Token vnodeEnd = vnodeIndex == vnodeBoundaries.size() ? cfs.partitioner.getMaximumToken() : vnodeBoundaries.get(vnodeIndex);
            long estimatedVNodeSize = estimateVNodeSize(nonExpiredSSTables, new Range<>(vnodeStart, vnodeEnd));
            if (estimatedVNodeSize >= vnodeSSTableSize)
            {
                l1writer.switchWriter(createWriter(location, estimatedTotalKeys, 1)); // todo: better estimate of total keys
                logger.info("writing vnode {} to L1, estimated size = {}, loc = {}", vnodeIndex, estimatedVNodeSize, l1writer.currentWriter().getFilename());
                activeWriter = l1writer;
            }
            else
            {
                activeWriter = sstableWriter;
                logger.info("writing vnode {} to L0, estimated size = {}, loc = {}", vnodeIndex, estimatedVNodeSize, sstableWriter.currentWriter().getFilename());
            }
        }

        private long estimateVNodeSize(Set<SSTableReader> nonExpiredSSTables, Range<Token> tokenRange)
        {
            long sum = 0;
            for (SSTableReader sstable : nonExpiredSSTables)
            {
                List<Pair<Long, Long>> positions = sstable.getPositionsForRanges(Arrays.asList(tokenRange));
                for (Pair<Long, Long> pos : positions)
                    sum += pos.right - pos.left;
            }
            double compressionRatio = cfs.metric.compressionRatio.getValue();
            double uncompressedSize = compactionGain * sum;
            return Math.round((compressionRatio > 0) ? compressionRatio * uncompressedSize : uncompressedSize);
        }

        @Override
        public Throwable doAbort(Throwable accumulate)
        {
            activeWriter = null;
            accumulate = super.doAbort(accumulate);
            return l1writer.abort(accumulate);
        }

        @Override
        protected Throwable doCommit(Throwable accumulate)
        {
            accumulate = super.doCommit(accumulate);
            return l1writer.commit(accumulate);
        }

        @Override
        protected void doPrepare()
        {
            super.doPrepare();
            l1writer.prepareToCommit();
        }

        @Override
        public List<SSTableReader> finish()
        {
            List<SSTableReader> finished = new ArrayList<>();
            finished.addAll(super.finish());
            finished.addAll(l1writer.finished());
            return finished;
        }

        @Override
        public List<SSTableReader> finish(long repairedAt)
        {
            List<SSTableReader> finished = new ArrayList<>();
            finished.addAll(sstableWriter.setRepairedAt(repairedAt).finish());
            finished.addAll(l1writer.setRepairedAt(repairedAt).finish());
            return finished;
        }

        private SSTableWriter createWriter(Directories.DataDirectory directory, long estimatedKeys, int level)
        {
            File sstableDirectory = cfs.directories.getLocationForDisk(directory);
            return SSTableWriter.create(Descriptor.fromFilename(cfs.getTempSSTablePath(sstableDirectory)),
                                        estimatedKeys,
                                        minRepairedAt,
                                        cfs.metadata,
                                        cfs.partitioner,
                                        new MetadataCollector(allSSTables, cfs.metadata.comparator, level));
        }
    }



    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);

        String size = options.containsKey(MIN_VNODE_SSTABLE_SIZE_MB) ? options.get(MIN_VNODE_SSTABLE_SIZE_MB) : "1";
        try
        {
            int ssSize = Integer.parseInt(size);
            if (ssSize < 0)
            {
                throw new ConfigurationException(String.format("%s must be >= 0, but was %s", MIN_VNODE_SSTABLE_SIZE_MB, ssSize));
            }
        }
        catch (NumberFormatException ex)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", size, MIN_VNODE_SSTABLE_SIZE_MB), ex);
        }

        uncheckedOptions.remove(MIN_VNODE_SSTABLE_SIZE_MB);

        uncheckedOptions = SizeTieredCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        return uncheckedOptions;
    }
}

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
package org.apache.cassandra.db;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSortedSet;

import org.apache.cassandra.io.util.DiskBoundTask;
import org.apache.cassandra.io.util.DiskBoundTaskExecutor;

/**
 * Task executor for writing SSTable to disk.
 * Tasks include flushing Memtable and running compaction(except validation).
 *
 * This class is responsible for determining which disk sstable should be written.
 */
public class DiskWriter extends AbstractExecutorService
{
    public static final DiskWriter instance = new DiskWriter();

    private final List<DiskBoundTaskExecutor> perDiskTaskExecutors;

    private DiskWriter()
    {
        perDiskTaskExecutors = new ArrayList<DiskBoundTaskExecutor>(Directories.dataFileLocations.size());
        for (Map.Entry<File, Integer> entry : Directories.dataFileLocations.entrySet())
        {
            perDiskTaskExecutors.add(new DiskBoundTaskExecutor(entry.getKey(),
                                                               entry.getValue(),
                                                               Integer.MAX_VALUE,
                                                               "DiskWriter-" + entry.getKey()));
        }
    }

    public void shutdown()
    {
        for (DiskBoundTaskExecutor executor : perDiskTaskExecutors)
            executor.shutdown();
    }

    public List<Runnable> shutdownNow()
    {
        List<Runnable> task = new ArrayList<Runnable>();
        for (DiskBoundTaskExecutor executor : perDiskTaskExecutors)
            task.addAll(executor.shutdownNow());
        return task;
    }

    public boolean isShutdown()
    {
        throw new UnsupportedOperationException();
    }

    public boolean isTerminated()
    {
        throw new UnsupportedOperationException();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    public Future<?> submit(Runnable command)
    {
        assert command instanceof DiskBoundTask : "DiskWriter only accept DiskBoundTask";

        DiskBoundTask task = (DiskBoundTask) command;
        ExecutorService executor = selectExecutor(task);

        if (executor != null)
            return executor.submit(task);
        else
            throw new RuntimeException("Insufficient disk space to write " + task.getExpectedWriteSize() + " bytes");
    }

    public void execute(Runnable command)
    {
        assert command instanceof DiskBoundTask : "DiskWriter only accept DiskBoundTask";

        DiskBoundTask task = (DiskBoundTask) command;
        ExecutorService executor = selectExecutor(task);

        if (executor != null)
            executor.execute(task);
        else
            throw new RuntimeException("Insufficient disk space to write " + task.getExpectedWriteSize() + " bytes");
    }

    private ExecutorService selectExecutor(DiskBoundTask task)
    {
        // sort by available disk space
        SortedSet<DiskBoundTaskExecutor> executors;
        synchronized (perDiskTaskExecutors)
        {
            executors = ImmutableSortedSet.copyOf(perDiskTaskExecutors);
        }

        // if there is disk with sufficient space and no activity running on it, then use it
        for (DiskBoundTaskExecutor executor : executors)
        {
            long spaceAvailable = executor.getEstimatedAvailableSpace();
            if (task.getExpectedWriteSize() < spaceAvailable && executor.getActiveCount() == 0)
                return executor;
        }

        // if not, use the one that has largest free space
        if (task.getExpectedWriteSize() < executors.first().getEstimatedAvailableSpace())
            return executors.first();
        else
            return task.recalculateWriteSize() ? selectExecutor(task) : null; // retry if needed
    }
}

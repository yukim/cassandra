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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DiskBoundTask;
import org.apache.cassandra.io.util.DiskBoundTaskExecutor;

/**
 * Task executor for flushing SSTable.
 *
 * This class is responsible for determining which disk sstable should be written.
 */
public class FlushWriter extends AbstractExecutorService
{
    private static Logger logger = LoggerFactory.getLogger(FlushWriter.class);

    private final DiskBoundTaskExecutor[] perDiskTaskExecutors;

    public FlushWriter()
    {
        perDiskTaskExecutors = new DiskBoundTaskExecutor[Directories.dataFileLocations.length];
        for (int i = 0; i < Directories.dataFileLocations.length; i++)
        {
            perDiskTaskExecutors[i] = new DiskBoundTaskExecutor(Directories.dataFileLocations[i],
                                                                1,
                                                                DatabaseDescriptor.getFlushQueueSize(),
                                                                "FlushWriter-" + Directories.dataFileLocations[i]);
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

    public void execute(Runnable command)
    {
        assert command instanceof DiskBoundTask : "FlushWriter only accept DiskBoundTask";

        DiskBoundTask task = (DiskBoundTask) command;
        long expectedSize = task.getExpectedWriteSize();
        long maxFreeDisk = 0;
        DiskBoundTaskExecutor executor = null;

        for (DiskBoundTaskExecutor dir : perDiskTaskExecutors)
        {
            if (maxFreeDisk < dir.getEstimatedAvailableSpace())
            {
                maxFreeDisk = dir.getEstimatedAvailableSpace();
                executor = dir;
            }
        }
        logger.debug(String.format("expected data files size is %d; largest free partition (%s) has %d bytes free",
                                          expectedSize, executor.disk, maxFreeDisk));

        if (executor == null)
            throw new RuntimeException("Insufficient disk space to flush " + expectedSize + " bytes");

        if (expectedSize < maxFreeDisk)
            executor.execute(task);
    }
}

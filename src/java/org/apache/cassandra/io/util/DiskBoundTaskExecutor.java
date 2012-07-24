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
package org.apache.cassandra.io.util;

import java.io.File;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.primitives.Longs;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.StageManager;

/**
 * Disk bound I/O task executor.
 */
public class DiskBoundTaskExecutor extends JMXEnabledThreadPoolExecutor implements Comparable<DiskBoundTaskExecutor>
{
    public final File disk;

    private final AtomicLong estimatedWorkingSize = new AtomicLong(0);

    public DiskBoundTaskExecutor(File disk, int threads, int queueSize, String name)
    {
        super(threads, StageManager.KEEPALIVE, TimeUnit.SECONDS,
              new LinkedBlockingQueue<Runnable>(queueSize),
              new NamedThreadFactory(name),
              "internal");

        this.disk = disk;
    }

    public Future<?> submit(Runnable command)
    {
        if (command instanceof DiskBoundTask)
        {
            DiskBoundTask task = (DiskBoundTask) command;
            estimatedWorkingSize.addAndGet(task.getExpectedWriteSize());
            task.bind(disk);
        }
        return super.submit(command);
    }

    public void execute(Runnable command)
    {
        if (command instanceof DiskBoundTask)
        {
            DiskBoundTask task = (DiskBoundTask) command;
            estimatedWorkingSize.addAndGet(task.getExpectedWriteSize());
            task.bind(disk);
        }
        super.execute(command);
    }

    /**
     * @return estimated available disk space for bounded directory,
     * excluding the expected size written by tasks in the queue
     */
    public long getEstimatedAvailableSpace()
    {
        // Load factor of 0.9 we do not want to use the entire disk that is too risky.
        return (long)(0.9 * disk.getUsableSpace()) - estimatedWorkingSize.get();
    }

    public int compareTo(DiskBoundTaskExecutor o)
    {
        return Longs.compare(getEstimatedAvailableSpace(), o.getEstimatedAvailableSpace());
    }

    protected void afterExecute(Runnable r, Throwable t)
    {
        if (r instanceof DiskBoundTask)
            estimatedWorkingSize.addAndGet(-1 * ((DiskBoundTask) r).getExpectedWriteSize());
        super.afterExecute(r, t);
    }
}

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

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.utils.WrappedRunnable;

public abstract class DiskBoundTask extends WrappedRunnable
{
    /**
     * Run this task after bind to certain disk.
     */
    protected void runMayThrow() throws Exception
    {
        long writeSize = getExpectedWriteSize();

        Directories.DataDirectory directory = selectDirectory(writeSize);
        if (directory == null)
            throw new RuntimeException("Insufficient disk space to write " + writeSize + " bytes");

        directory.currentTasks.incrementAndGet();
        directory.estimatedWorkingSize.addAndGet(writeSize);
        try
        {
            executeOn(directory.location);
        }
        finally
        {
            directory.estimatedWorkingSize.addAndGet(-1 * writeSize);
            directory.currentTasks.decrementAndGet();
        }
    }

    /**
     * Executes this task on given {@code dataDirectory}.
     * @param dataDirectory data directory to work on
     */
    protected abstract void executeOn(File dataDirectory) throws Exception;

    /**
     * Get expected write size to determine which disk to use for this task.
     * @return expected size in bytes this task will write to disk.
     */
    public abstract long getExpectedWriteSize();

    /**
     * @return true if calculate expected write size again.
     */
    public boolean recalculateWriteSize()
    {
        return false;
    }

    private Directories.DataDirectory selectDirectory(long expectedWriteSize)
    {
        Directories.DataDirectory disk = Directories.getLocationCapableOfSize(expectedWriteSize);
        if (disk == null)
            disk = recalculateWriteSize() ? selectDirectory(expectedWriteSize) : null; // retry if needed
        return disk;
    }
}

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

import org.apache.cassandra.utils.WrappedRunnable;

/**
 * Use with DiskBoundTaskExecutor to perform disk bound IO task.
 */
public abstract class DiskBoundTask extends WrappedRunnable
{
    protected File currentDisk;

    /**
     * Bind this task to given disk.
     * This method is called before {@link org.apache.cassandra.utils.WrappedRunnable#runMayThrow()}
     * is invoked, so that task can access current bound disk through currentDisk.
     *
     * @param disk disk to bind
     */
    public void bind(File disk)
    {
        this.currentDisk = disk;
    }

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
}

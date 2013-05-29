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
package org.apache.cassandra.streaming;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.streaming.messages.StreamMessageListener;

public abstract class ProgressSupport
{
    public static final byte IN = 1;
    public static final byte OUT = 0;

    private static final long PROGRESS_EVENT_INTERVAL_IN_MS = 1000L;

    private final byte direction;
    private final StreamMessageListener listener;

    // time in ms when last ProgressEvent is fired
    private long lastFiredAt;

    private boolean done;

    protected ProgressSupport(byte direction, StreamMessageListener listener)
    {
        this.direction = direction;
        this.listener = listener;
    }

    /**
     * Only fire ProgressEvent at specific period or when complete (progress == totalSize)
     *
     * @param progress progress so far (in bytes)
     */
    protected void maybeFireProgressEvent(Descriptor desc, long progress)
    {
        long size = totalSize();
        if (!done)
        {
            if ((done = size == progress) || System.currentTimeMillis() - lastFiredAt > PROGRESS_EVENT_INTERVAL_IN_MS)
            {
                listener.onStreamProgress(desc, direction, progress, size);
                lastFiredAt = System.currentTimeMillis();
            }
        }
    }

    protected abstract long totalSize();
}

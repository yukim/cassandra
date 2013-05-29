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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.streaming.messages.FileMessage;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.utils.CFPath;
import org.apache.cassandra.utils.Pair;

/**
 * Task that sends files for certain keyspace, columnfamily and SSTable files.
 */
public class StreamTransferTask extends StreamTask
{
    private final AtomicInteger sequenceNumber = new AtomicInteger(0);

    /** Streaming files, grouped by ColumnFamily name */
    private final Map<FileMessageHeader, FileMessage> files = new HashMap<>();

    private long totalSize;

    public StreamTransferTask(StreamSession session, CFPath path)
    {
        super(session, path);
    }

    public void addTransferFile(SSTableReader sstable, long estimatedKeys, List<Pair<Long, Long>> sections)
    {
        FileMessage message = new FileMessage(sstable, sequenceNumber.incrementAndGet(), estimatedKeys, sections);
        files.put(message.header, message);
        totalSize += message.header.size();
    }

    /**
     * Complete sending file.
     *
     * @param header FileMessageHeader of sent file
     */
    public void complete(FileMessageHeader header)
    {
        files.remove(header);
        // all file sent, notify session this task is complete.
        if (files.isEmpty())
            session.taskCompleted(this);
    }

    public int getTotalNumberOfFiles()
    {
        return files.size();
    }

    public long getTotalSize()
    {
        return totalSize;
    }

    public Collection<FileMessage> getFileMessages()
    {
        return files.values();
    }

    public FileMessage createMessageForRetry(int sequenceNumber)
    {
        // TODO retry
        FileMessageHeader key = new FileMessageHeader(path, sequenceNumber, 0, null, null);
        assert files.containsKey(key);
        return files.get(key);
    }
}

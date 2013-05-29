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
package org.apache.cassandra.streaming.messages;

import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.CFPath;

/**
 * Callback interface for handling {@link StreamMessage}s and connection.
 */
public interface StreamMessageListener
{
    /**
     * Call back for connection success
     */
    void onConnect();

    /**
     * Call back for handling exception during streaming.
     *
     * @param e thrown exception
     */
    void onError(Throwable e);

    /**
     * Call back for receiving prepare message.
     */
    void onPrepareReceived(Collection<StreamRequest> requests, Collection<StreamSummary> summaries);

    /**
     * Call back after sending FileMessageHeader.
     *
     * @param header sent header
     * @param sstable SSTable to send
     * @param out Channel for writing SSTable
     */
    void onFileSend(FileMessageHeader header, SSTableReader sstable, WritableByteChannel out);

    /**
     * Call back after receiving FileMessageHeader.
     *
     * @param header received header
     * @param in Used for receiving actual data
     */
    void onFileReceive(FileMessageHeader header, ReadableByteChannel in);

    /**
     * Call back on streaming/receiving file in progress.
     *
     * @param desc SSTable descriptor
     * @param direction stream direction (0 for out, 1 for in)
     * @param bytes bytes transferred/received so far
     * @param total total bytes to transfer/receive
     */
    void onStreamProgress(Descriptor desc, byte direction, long bytes, long total);

    /**
     * Call back on receiving {@code StreamMessage.Type.RETRY} message.
     *
     * @param path Keyspace/ColumnFamily
     * @param sequenceNumber Sequence number to indicate which file to stream again
     */
    void onRetryReceived(CFPath path, int sequenceNumber);

    /**
     * Call back on receiving {@code StreamMessage.Type.COMPLETE} message.
     */
    void onCompleteReceived();

    /**
     * Call back on receiving {@code StreamMessage.Type.SESSION_FAILED} message.
     */
    void onSessionFailedReceived();
}

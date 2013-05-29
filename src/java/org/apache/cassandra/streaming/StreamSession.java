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

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.*;
import java.util.concurrent.Future;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.streaming.management.StreamEvent;
import org.apache.cassandra.streaming.messages.*;
import org.apache.cassandra.streaming.messages.StreamRequest;
import org.apache.cassandra.utils.CFPath;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * StreamSession groups several StreamTasks that deal with the same peer.
 *
 * Session is identified by UUID and has list of tasks.
 *
 * StreamSession manages receiving files and sending files through StreamTask.
 *
 * StreamSession goes through several stages.
 *
 * 1. Init
 *    StreamSession in one end send init message to the other end.
 * 2. Prepare
 *    StreamSession in both endpoints are created, so in this phase, they exchange
 *    request and summary messages to prepare receiving/streaming files in next phase.
 * 3. Stream
 *    StreamSessions in both ends stream and receive files.
 *    Retrying stream is done in this phase.
 * 4. Complete
 *    Session completes if both endpoints completed by exchanging complete message.
 */
public class StreamSession implements Runnable, StreamMessageListener
{
    private static final Logger logger = LoggerFactory.getLogger(StreamSession.class);

    public final StreamOperation operation;
    public final InetAddress peer;

    // streaming tasks are created and managed per keyspace
    private final List<StreamRequest> requests = new ArrayList<>();
    private final Map<CFPath, StreamTransferTask> transfers = new HashMap<>();
    // data receivers, filled after receiving prepare message
    private final Map<CFPath, StreamReceiveTask> receivers = new HashMap<>();

    public final ConnectionHandler handler;

    private int retries;

    private volatile boolean prepared = false;
    private volatile boolean completed = false;
    private volatile boolean peerCompleted = false;

    /**
     * Create new streaming session with the peer.
     *
     * @param operation StreamOperation that this session belongs
     * @param peer Address of streaming peer
     */
    StreamSession(StreamOperation operation, InetAddress peer)
    {
        this.operation = operation;
        this.peer = peer;
        this.handler = new ConnectionHandler(this);
    }

    /**
     * Request data fetch task to this session.
     *
     * @param keyspace Requesting keyspace
     * @param ranges Ranges to retrieve data
     * @param columnFamilies ColumnFamily names. Can be empty if requesting all CF under the keyspace.
     */
    public void addStreamRequest(String keyspace, Collection<Range<Token>> ranges, Collection<String> columnFamilies)
    {
        requests.add(new StreamRequest(keyspace, ranges, columnFamilies));
    }

    /**
     * Set up transfer for specific keyspace/ranges/CFs
     *
     * @param keyspace Transfer keyspace
     * @param ranges Transfer ranges
     * @param columnFamilies Transfer ColumnFamilies
     */
    public void addTransferRanges(String keyspace, Collection<Range<Token>> ranges, Collection<String> columnFamilies, boolean flushTables)
    {
        Collection<ColumnFamilyStore> stores = new HashSet<>();
        // if columnfamilies are not specified, we add all cf under the keyspace
        if (columnFamilies.isEmpty())
        {
            stores.addAll(Table.open(keyspace).getColumnFamilyStores());
        }
        else
        {
            for (String cf : columnFamilies)
                stores.add(Table.open(keyspace).getColumnFamilyStore(cf));
        }

        if (flushTables)
            flushSSTables(stores);

        List<SSTableReader> sstables = Lists.newLinkedList();
        for (ColumnFamilyStore cfStore : stores)
        {
            List<AbstractBounds<RowPosition>> rowBoundsList = Lists.newLinkedList();
            for (Range<Token> range : ranges)
                rowBoundsList.add(range.toRowBounds());
            ColumnFamilyStore.ViewFragment view = cfStore.markReferenced(rowBoundsList);
            sstables.addAll(view.sstables);
        }
        addTransferFiles(ranges, sstables);
    }

    /**
     * Set up transfer of the specific SSTables.
     * {@code sstables} must be marked as referenced so that not get deleted until transfer completes.
     *
     * @param ranges Transfer ranges
     * @param sstables Transfer files
     */
    public void addTransferFiles(Collection<Range<Token>> ranges, Collection<SSTableReader> sstables)
    {
        for (SSTableReader sstable : sstables)
        {
            List<Pair<Long, Long>> sections = sstable.getPositionsForRanges(ranges);
            if (sections.isEmpty())
            {
                // A reference was acquired on the sstable and we won't stream it
                sstable.releaseReference();
                continue;
            }
            long estimatedKeys = sstable.estimatedKeysForRanges(ranges);
            CFPath path = new CFPath(sstable.descriptor.ksname, sstable.getColumnFamilyName());
            StreamTransferTask task = transfers.get(path);
            if (task == null)
            {
                task = new StreamTransferTask(this, path);
                transfers.put(path, task);
            }
            task.addTransferFile(sstable, estimatedKeys, sections);
        }
    }

    /**
     * Start stream session by sending StreamInit message.
     *
     * Executed only on the stream initiator.
     */
    public void run()
    {
        if (requests.isEmpty() && transfers.isEmpty())
        {
            logger.debug("Session does not have any tasks.");
            operation.handleSessionComplete(this);
        }
        else
        {
            try
            {
                handler.connect();
            }
            catch (IOException e)
            {
                onError(e);
            }
        }
    }

    /**
     * @return true if this session has prepared.
     */
    public boolean isPrepared()
    {
        return prepared;
    }

    public boolean isSuccess()
    {
        return completed && peerCompleted;
    }

    /**
     * When connected, session moves to preparing phase and sends prepare message.
     */
    public void onConnect()
    {
        logger.debug("Connected. Sending prepare...");
        // send prepare message
        PrepareMessage prepare = new PrepareMessage();
        prepare.requests.addAll(requests);
        for (StreamTransferTask task : transfers.values())
            prepare.summaries.add(task.getSummary());
        handler.sendMessage(prepare);

        // if we don't need to prepare for stream request, start sending files
        if (requests.isEmpty())
        {
            prepared = true;
            logger.debug("Prepare complete. Start streaming files.");
            startStreamingFiles();
        }
    }

    public void onError(Throwable e)
    {
        logger.error("onError", e);
        // send session failure message
        handler.sendMessage(new SessionFailedMessage());
        // fail session
        operation.handleSessionComplete(this);
    }

    public void onPrepareReceived(Collection<StreamRequest> requests, Collection<StreamSummary> summaries)
    {
        assert !prepared;

        logger.debug("Start preparing this session (" + requests.size() + " requests, " + summaries.size() + " columnfamilies receiving)");
        // prepare tasks
        for (StreamRequest request : requests)
            addTransferRanges(request.keyspace, request.ranges, request.columnFamilies, true); // always flush on stream request
        for (StreamSummary summary : summaries)
            prepareReceiving(summary);

        // send back prepare message if prepare message contains stream request
        if (!requests.isEmpty())
        {
            PrepareMessage prepare = new PrepareMessage();
            for (StreamTransferTask task : transfers.values())
                prepare.summaries.add(task.getSummary());
            handler.sendMessage(prepare);
        }

        prepared = true;
        logger.debug("Prepare complete. Start streaming files.");
        startStreamingFiles();
    }

    public void onFileSend(FileMessageHeader header, SSTableReader sstableToTransfer, WritableByteChannel out)
    {
        RateLimiter limiter = StreamManager.instance.getRateLimiter(peer);
        StreamWriter writer = header.compressionInfo == null ?
                                      new StreamWriter(sstableToTransfer, header.sections, this, limiter) :
                                      new CompressedStreamWriter(sstableToTransfer, header.sections, header.compressionInfo, this, limiter);
        try
        {
            writer.write(out);
            transfers.get(header.path).complete(header);
        }
        catch (Throwable e)
        {
            onError(e);
        }
    }

    public void onFileReceive(FileMessageHeader header, ReadableByteChannel in)
    {
        StreamReader reader = header.compressionInfo == null ? new StreamReader(header, this)
                                                             : new CompressedStreamReader(header, this);
        try
        {
            receivers.get(header.path).receive(reader.read(in));
        }
        catch (Throwable e)
        {
            // retry
            retries++;
            if (retries > DatabaseDescriptor.getMaxStreamingRetries())
                onError(new IOException("Too many retries for " + header, e));
            else
                handler.sendMessage(new RetryMessage(header.path, header.sequenceNumber));
        }
    }

    public void onStreamProgress(Descriptor desc, byte direction, long bytes, long total)
    {
        fireStreamEvent(new StreamEvent.ProgressEvent(this, desc.filenameFor(Component.DATA), direction, bytes, total));
    }

    public void onRetryReceived(CFPath path, int sequenceNumber)
    {
        FileMessage message = transfers.get(path).createMessageForRetry(sequenceNumber);
        handler.sendMessage(message);
    }

    public synchronized void onCompleteReceived()
    {
        peerCompleted = true;
        logger.debug("onCompleteReceived this:" + completed + ", peer:" + peerCompleted);
        if (this.completed)
        {
            handler.close();
            operation.handleSessionComplete(this);
        }
    }

    public synchronized void onSessionFailedReceived()
    {
        logger.debug("onSessionReceived");
        handler.close();
        operation.handleSessionComplete(this);
    }

    public List<StreamSummary> getReceivingSummaries()
    {
        List<StreamSummary> summaries = Lists.newArrayList();
        for (StreamTask receiver : receivers.values())
            summaries.add(receiver.getSummary());
        return summaries;
    }

    public List<StreamSummary> getTransferSummaries()
    {
        List<StreamSummary> summaries = Lists.newArrayList();
        for (StreamTask transfer : transfers.values())
            summaries.add(transfer.getSummary());
        return summaries;
    }

    public synchronized void taskCompleted(StreamReceiveTask completedTask)
    {
        receivers.remove(completedTask.path);
        maybeCompleted();
    }

    public synchronized void taskCompleted(StreamTransferTask completedTask)
    {
        transfers.remove(completedTask.path);
        maybeCompleted();
    }

    public void maybeCompleted()
    {
        logger.debug((receivers.size() + transfers.size()) + " remaining...");
        if (receivers.isEmpty() && transfers.isEmpty())
        {
            this.completed = true;
            logger.debug("onAllTaskComplete this:" + completed + ", peer:" + peerCompleted);
            handler.sendMessage(new CompleteMessage());
            if (peerCompleted)
            {
                handler.close();
                operation.handleSessionComplete(this);
            }
        }
    }

    public ByteBuffer createStreamInitMessage(int version)
    {
        StreamInitMessage message = new StreamInitMessage(operation.operationId, operation.type);
        return message.createMessage(false, version);
    }

    protected void fireStreamEvent(StreamEvent event)
    {
        operation.fireStreamEvent(event);
    }

    /**
     * Flushes matching column families from the given keyspace, or all columnFamilies
     * if the cf list is empty.
     */
    private void flushSSTables(Iterable<ColumnFamilyStore> stores)
    {
        logger.info("Flushing memtables for {}...", stores);
        List<Future<?>> flushes = new ArrayList<>();
        for (ColumnFamilyStore cfs : stores)
            flushes.add(cfs.forceFlush());
        FBUtilities.waitOnFutures(flushes);
    }

    private void prepareReceiving(StreamSummary summary)
    {
        logger.debug("prepare receiving " + summary);
        if (summary.files > 0)
            receivers.put(summary.path, new StreamReceiveTask(this, summary.path, summary.files, summary.totalSize));
    }

    private void startStreamingFiles()
    {
        assert prepared;

        fireStreamEvent(new StreamEvent.SessionPreparedEvent(this));
        for (StreamTransferTask task : transfers.values())
        {
            if (task.getFileMessages().size() > 0)
                handler.sendMessages(task.getFileMessages());
            else
                taskCompleted(task);
        }
    }
}

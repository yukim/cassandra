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

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.streaming.management.StreamEvent;
import org.apache.cassandra.streaming.management.StreamEventHandler;
import org.apache.cassandra.utils.SimpleCondition;

/**
 * StreamOperation represents the operation the node performs involving both inbound and outbound file transfers.
 * When you perform operation like repair, bootstrap, bulkload, etc, you create StreamOperation instance for each operation.
 * Then, you add {@link StreamTask}s to this operation.
 * {@code StreamTask}s are managed by each destination, under {@link StreamSession} object.
 */
public class StreamOperation implements Callable<Boolean>, IEndpointStateChangeSubscriber, IFailureDetectionEventListener
{
    private static final Logger logger = LoggerFactory.getLogger(StreamOperation.class);

    /**
     * Operation ID identifies each operation. It will be generated if not given.
     */
    public final UUID operationId;

    public final OperationType type;

    // running sessions are add and managed by InetAddress of the other end.
    private final Map<InetAddress, StreamSession> sessions = new HashMap<>();

    private final List<StreamEventHandler> eventListeners = Collections.synchronizedList(new ArrayList<StreamEventHandler>());

    private final Set<StreamSession> failedSessions = new HashSet<>();

    // synchronizer used to wait for all sessions to complete
    private final SimpleCondition sync = new SimpleCondition();

    private volatile boolean success = false;

    // when flushBeforeTransfer is true, will flush before streaming ranges
    private boolean flushBeforeTransfer = true;

    /**
     * Create new StreamOperation of given id and type.
     *
     * @param operationId operation id
     * @param type Stream operation type
     */
    StreamOperation(UUID operationId, OperationType type)
    {
        this.operationId = operationId;
        this.type = type;
    }

    /**
     * Request data in {@code keyspace} and {@code ranges} from specific node.
     *
     * @param from endpoint address to fetch data from.
     * @param keyspace name of keyspace
     * @param ranges ranges to fetch
     */
    public void requestRanges(InetAddress from, String keyspace, Collection<Range<Token>> ranges)
    {
        requestRanges(from, keyspace, ranges, new String[0]);
    }

    public void requestRanges(InetAddress from, String keyspace, Collection<Range<Token>> ranges, String... columnFamilies)
    {
        StreamSession session = getOrCreateSession(from);
        session.addStreamRequest(keyspace, ranges, Arrays.asList(columnFamilies));
    }

    /**
     * Add transfer task to send data of specific keyspace and ranges.
     *
     * @param to endpoint address of receiver
     * @param keyspace name of keyspace
     * @param ranges ranges to send
     */
    public void transferRanges(InetAddress to, String keyspace, Collection<Range<Token>> ranges)
    {
        transferRanges(to, keyspace, ranges, new String[0]);
    }

    public void transferRanges(InetAddress to, String keyspace, Collection<Range<Token>> ranges, String... columnFamilies)
    {
        StreamSession session = getOrCreateSession(to);
        session.addTransferRanges(keyspace, ranges, Arrays.asList(columnFamilies), flushBeforeTransfer);
    }

    /**
     * Add transfer task to send given SSTable files.
     *
     * @param to endpoint address of receiver
     * @param ranges ranges to send
     * @param sstables files to send
     */
    public void transferFiles(InetAddress to, Collection<Range<Token>> ranges, Collection<SSTableReader> sstables)
    {
        StreamSession session = getOrCreateSession(to);
        session.addTransferFiles(ranges, sstables);
    }

    public StreamSession getOrCreateSession(InetAddress peer)
    {
        StreamSession session = sessions.get(peer);
        if (session == null)
        {
            session = new StreamSession(this, peer);
            sessions.put(peer, session);
        }
        return session;
    }

    public void addEventListener(StreamEventHandler listener)
    {
        eventListeners.add(listener);
    }

    /**
     * Start all sessions.
     *
     * @return true if all sessions succeeded
     * @throws Exception
     */
    public Boolean call() throws Exception
    {
        // TODO change stage to someting meaningful
        // start each stream session
        for (StreamSession session : sessions.values())
            StageManager.getStage(Stage.MISC).submit(session);
        sync.await();
        return success;
    }

    /**
     * Start this operation.
     *
     * @return Future object which returns true when operation succeeded.
     */
    public Future<Boolean> start()
    {
        FutureTask<Boolean> task = new FutureTask<>(this);
        task.run();
        return task;
    }

    /**
     * @param flushBeforeTransfer set to true when the node should flush before transfer
     * @return this
     */
    public StreamOperation flushBeforeTransfer(boolean flushBeforeTransfer)
    {
        this.flushBeforeTransfer = flushBeforeTransfer;
        return this;
    }

    public void onJoin(InetAddress endpoint, EndpointState epState) {}
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {}
    public void onAlive(InetAddress endpoint, EndpointState state) {}
    public void onDead(InetAddress endpoint, EndpointState state) {}

    public void onRemove(InetAddress endpoint)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    public void onRestart(InetAddress endpoint, EndpointState epState)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    public void convict(InetAddress endpoint, double phi)
    {
        StreamSession session = sessions.get(endpoint);
        if (session == null)
            return;

        // We want a higher confidence in the failure detection than usual because failing a streaming wrongly has a high cost.
        if (phi < 2 * DatabaseDescriptor.getPhiConvictThreshold())
            return;

        handleSessionComplete(session);
    }

    public Collection<StreamSession> getLiveSessions()
    {
        return sessions.values();
    }

    public void handleSessionComplete(StreamSession session)
    {
        StreamSession completed = sessions.remove(session.peer);
        if (!completed.isSuccess())
        {
            logger.warn("Session with " + completed.peer + " failed");
            failedSessions.add(completed);
        }
        logger.info("Session complete (remaining: " + sessions.size() + ")");
        fireStreamEvent(new StreamEvent.SessionCompleteEvent(completed));
        maybeComplete();
    }

    private void maybeComplete()
    {
        if (sessions.isEmpty())
        {
            logger.info("Operation complete");
            success = failedSessions.isEmpty();
            fireStreamEvent(new StreamEvent.OperationCompleteEvent(this));
            sync.signalAll();
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamOperation that = (StreamOperation) o;
        return operationId.equals(that.operationId);
    }

    @Override
    public int hashCode()
    {
        return operationId.hashCode();
    }

    protected void fireStreamEvent(StreamEvent event)
    {
        for (StreamEventHandler listener : eventListeners)
            listener.handleStreamEvent(event);
    }
}

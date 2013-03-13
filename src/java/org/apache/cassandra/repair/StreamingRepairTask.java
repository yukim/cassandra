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
package org.apache.cassandra.repair;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.*;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Task that make two nodes exchange (stream) some ranges (for a given table/cf).
 * This handle the case where the local node is neither of the two nodes that
 * must stream their range, and allow to register a callback to be called on
 * completion.
 */
public class StreamingRepairTask implements Runnable, IStreamCallback
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingRepairTask.class);

    /** Repair session ID that this streaming task belongs */
    public final RepairJobDesc desc;
    public final SyncRequest request;

    // we expect one callback for the receive, and one for the send
    private final AtomicInteger outstanding = new AtomicInteger(2);

    public StreamingRepairTask(RepairJobDesc desc, SyncRequest request)
    {
        this.desc = desc;
        this.request = request;
    }

    /**
     * Returns true if the task if the task can be executed locally, false if
     * it has to be forwarded.
     */
    public boolean isLocalTask()
    {
        return request.initiator.equals(request.src);
    }

    public void run()
    {
        if (request.src.equals(FBUtilities.getBroadcastAddress()))
        {
            initiateStreaming();
        }
        else
        {
            forwardToSource();
        }
    }

    private void initiateStreaming()
    {
        ColumnFamilyStore cfstore = Table.open(desc.keyspace).getColumnFamilyStore(desc.columnFamily);
        try
        {
            logger.info(String.format("[repair #%s] Performing streaming repair of %d ranges with %s", desc.sessionId, request.ranges.size(), request.dst));
            // We acquire references for transferSSTables
            Collection<SSTableReader> sstables = cfstore.markCurrentSSTablesReferenced();
            // send ranges to the remote node
            StreamOutSession outsession = StreamOutSession.create(desc.keyspace, request.dst, this);
            StreamOut.transferSSTables(outsession, sstables, request.ranges, OperationType.AES);
            // request ranges from the remote node
            StreamIn.requestRanges(request.dst, desc.keyspace, Collections.singleton(cfstore), request.ranges, this, OperationType.AES);
        }
        catch(Exception e)
        {
            throw new RuntimeException("Streaming repair failed", e);
        }
    }

    private void forwardToSource()
    {
        logger.info(String.format("[repair #%s] Forwarding streaming repair of %d ranges to %s (to be streamed with %s)", desc.sessionId, request.ranges.size(), request.src, request.dst));
        RepairMessageHeader header = new RepairMessageHeader(desc, RepairMessageType.SYNC_REQUEST);
        MessagingService.instance().sendOneWay(new RepairMessage<>(header, request).createMessage(), request.src);
    }

    /**
     * If we succeeded on both stream in and out, reply back to the initiator.
     */
    public void onSuccess()
    {
        if (outstanding.decrementAndGet() > 0)
            return; // waiting on more calls

        logger.info(String.format("[repair #%s] streaming task succeed, returning response to %s", desc.sessionId, request.initiator));
        if (MessagingService.instance().getVersion(request.initiator) < MessagingService.VERSION_20)
        {
            // we have to respond with older format
        }
        else
        {
            RepairMessageHeader header = new RepairMessageHeader(desc, RepairMessageType.SYNC_COMPLETE);
            NodePair nodes = new NodePair(request.src, request.dst);
            MessagingService.instance().sendOneWay(new RepairMessage<>(header, nodes).createMessage(), request.initiator);
        }
    }

    /**
     * If we failed on either stream in or out, reply fail to the initiator.
     */
    public void onFailure()
    {
        RepairMessageHeader header = new RepairMessageHeader(desc, RepairMessageType.SYNC_FAILED);
        NodePair nodes = new NodePair(request.src, request.dst);
        MessagingService.instance().sendOneWay(new RepairMessage<>(header, nodes).createMessage(), request.initiator);
    }
}

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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;

/**
 * Runs on the node that initiated a request to compare two trees, and launch repairs for disagreeing ranges.
 */
public class Differencer implements Runnable
{
    private static Logger logger = LoggerFactory.getLogger(ActiveRepairService.class);

    private final RepairJobDesc desc;
    public final TreeResponse r1;
    public final TreeResponse r2;
    public final List<Range<Token>> differences = new ArrayList<>();

    public Differencer(RepairJobDesc desc, TreeResponse r1, TreeResponse r2)
    {
        this.desc = desc;
        this.r1 = r1;
        this.r2 = r2;
    }

    /**
     * Compares our trees, and triggers repairs for any ranges that mismatch.
     */
    public void run()
    {
        // restore partitioners (in case we were serialized)
        if (r1.tree.partitioner() == null)
            r1.tree.partitioner(StorageService.getPartitioner());
        if (r2.tree.partitioner() == null)
            r2.tree.partitioner(StorageService.getPartitioner());

        // compare trees, and collect differences
        differences.addAll(MerkleTree.difference(r1.tree, r2.tree));

        // choose a repair method based on the significance of the difference
        String format = String.format("[repair #%s] Endpoints %s and %s %%s for %s", desc.sessionId, r1.endpoint, r2.endpoint, desc.columnFamily);
        if (differences.isEmpty())
        {
            logger.info(String.format(format, "are consistent"));
            // send back sync complete message
            RepairMessageHeader header = new RepairMessageHeader(desc, RepairMessageType.SYNC_COMPLETE);
            NodePair nodes = new NodePair(r1.endpoint, r2.endpoint);
            MessagingService.instance().sendOneWay(new RepairMessage<>(header, nodes).createMessage(), FBUtilities.getLocalAddress());
            return;
        }

        // non-0 difference: perform streaming repair
        logger.info(String.format(format, "have " + differences.size() + " range(s) out of sync"));
        performStreamingRepair();
    }

    /**
     * Starts sending/receiving our list of differences to/from the remote endpoint: creates a callback
     * that will be called out of band once the streams complete.
     */
    void performStreamingRepair()
    {
        SyncRequest request = SyncRequest.create(r1.endpoint, r2.endpoint, differences);
        StreamingRepairTask task = new StreamingRepairTask(desc, request);

        // Pre 1.0, nodes don't know how to handle forwarded streaming task so don't bother
        if (task.isLocalTask() || MessagingService.instance().getVersion(task.request.dst) >= MessagingService.VERSION_10)
            task.run();
    }

    /**
     * In order to remove completed Differencer, equality is computed only from {@code desc} and
     * endpoint part of two TreeResponses.
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Differencer that = (Differencer) o;
        if (!desc.equals(that.desc)) return false;
        if (r1.endpoint.equals(that.r1.endpoint))
        {
            return r2.endpoint.equals(that.r2.endpoint);
        }
        else if (r1.endpoint.equals(that.r2.endpoint))
        {
            return r2.endpoint.equals(that.r1.endpoint);
        }
        else
        {
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        int result = desc.hashCode();
        result = 31 * result + r1.endpoint.hashCode() + r2.endpoint.hashCode();
        return result;
    }

    public String toString()
    {
        return "#<Differencer " + r1.endpoint + "<->" + r2.endpoint + "/" + desc.range + ">";
    }
}

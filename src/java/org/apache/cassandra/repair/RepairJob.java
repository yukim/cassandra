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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Condition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.SnapshotCommand;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.SimpleCondition;

/**
 * RepairJob runs repair on given ColumnFamily.
 */
public class RepairJob
{
    private static Logger logger = LoggerFactory.getLogger(ActiveRepairService.class);

    public final RepairJobDesc desc;
    private final boolean isSequential;
    // first we send tree requests. this tracks the endpoints remaining to hear from
    private final RequestCoordinator<InetAddress> treeRequests;
    // tree responses are then tracked here
    private final List<TreeResponse> trees;
    // once all responses are received, each tree is compared with each other, and differencer tasks
    // are submitted. the job is done when all differencers are complete.
    private final RequestCoordinator<Differencer> differencers;
    private final Condition requestsSent = new SimpleCondition();
    private CountDownLatch snapshotLatch = null;

    /**
     * Create repair job to run on specific columnfamily
     *
     * @param repairSession
     * @param columnFamily name of the columnfamily
     */
    public RepairJob(RepairSession repairSession, String columnFamily)
    {
        this.desc = new RepairJobDesc(repairSession.getId(), repairSession.keyspace, columnFamily, repairSession.getRange());
        this.isSequential = repairSession.isSequential;
        this.trees = new ArrayList<>(repairSession.endpoints.size() + 1);
        this.treeRequests = new RequestCoordinator<InetAddress>(isSequential)
        {
            public void send(InetAddress endpoint)
            {
                RepairMessage message = new RepairMessage(new RepairMessageHeader(desc, RepairMessageType.VALIDATION_REQUEST));
                MessagingService.instance().sendOneWay(message.createMessage(), endpoint);
            }
        };
        this.differencers = new RequestCoordinator<Differencer>(isSequential)
        {
            public void send(Differencer d)
            {
                StageManager.getStage(Stage.ANTI_ENTROPY).execute(d);
            }
        };
    }

    /**
     * Send merkle tree request to every involved neighbor.
     */
    public void sendTreeRequests(Collection<InetAddress> endpoints)
    {
        // send requests to all nodes
        List<InetAddress> allEndpoints = new ArrayList<>(endpoints);
        allEndpoints.add(FBUtilities.getBroadcastAddress());

        if (isSequential)
            makeSnapshots(endpoints);

        for (InetAddress endpoint : allEndpoints)
            treeRequests.add(endpoint);

        logger.info(String.format("[repair #%s] requesting merkle trees for %s (to %s)", desc.sessionId, desc.columnFamily, allEndpoints));
        treeRequests.start();
        requestsSent.signalAll();
    }

    public void makeSnapshots(Collection<InetAddress> endpoints)
    {
        try
        {
            snapshotLatch = new CountDownLatch(endpoints.size());
            IAsyncCallback callback = new IAsyncCallback()
            {
                public boolean isLatencyForSnitch()
                {
                    return false;
                }

                public void response(MessageIn msg)
                {
                    RepairJob.this.snapshotLatch.countDown();
                }
            };
            for (InetAddress endpoint : endpoints)
                MessagingService.instance().sendRR(new SnapshotCommand(desc.keyspace, desc.columnFamily, desc.sessionId.toString(), false).createMessage(), endpoint, callback);
            snapshotLatch.await();
            snapshotLatch = null;
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Add a new received tree and return the number of remaining tree to
     * be received for the job to be complete.
     *
     * Callers may assume exactly one addTree call will result in zero remaining endpoints.
     */
    public synchronized int addTree(InetAddress endpoint, MerkleTree tree)
    {
        // Wait for all request to have been performed (see #3400)
        try
        {
            requestsSent.await();
        }
        catch (InterruptedException e)
        {
            throw new AssertionError("Interrupted while waiting for requests to be sent");
        }
        trees.add(new TreeResponse(endpoint, tree));
        return treeRequests.completed(endpoint);
    }

    /**
     * Submit differencers for running.
     * All tree *must* have been received before this is called.
     */
    public void submitDifferencers()
    {
        // We need to difference all trees one against another
        for (int i = 0; i < trees.size() - 1; ++i)
        {
            TreeResponse r1 = trees.get(i);
            for (int j = i + 1; j < trees.size(); ++j)
            {
                TreeResponse r2 = trees.get(j);
                Differencer differencer = new Differencer(desc, r1, r2);
                logger.debug("Queueing comparison {}", differencer);
                differencers.add(differencer);
            }
        }
        differencers.start();
        trees.clear(); // allows gc to do its thing
    }

    /**
     * @return true if the @param differencer was the last remaining
     */
    synchronized boolean completedSynchronization(NodePair nodes)
    {
        Differencer completed = new Differencer(desc, new TreeResponse(nodes.endpoint1, null), new TreeResponse(nodes.endpoint2, null));
        return differencers.completed(completed) == 0;
    }

    /**
     * terminate this job.
     */
    public void terminate()
    {
        if (snapshotLatch != null)
        {
            while (snapshotLatch.getCount() > 0)
                snapshotLatch.countDown();
        }
    }
}

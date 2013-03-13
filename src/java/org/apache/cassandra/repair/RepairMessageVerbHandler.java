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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.MerkleTree;

/**
 * Handles all repair related message.
 *
 * @since 2.0
 */
public class RepairMessageVerbHandler implements IVerbHandler<RepairMessage<?>>
{
    public void doVerb(MessageIn<RepairMessage<?>> message, int id)
    {
        RepairMessageHeader header = message.payload.header;
        switch (header.type)
        {
            case VALIDATION_REQUEST:
                // trigger read-only compaction
                ColumnFamilyStore store = Table.open(header.desc.keyspace).getColumnFamilyStore(header.desc.columnFamily);
                // TODO: memory usage (maxsize) should either be tunable per
                // CF, globally, or as shared for all CFs in a cluster
                MerkleTree tree = new MerkleTree(DatabaseDescriptor.getPartitioner(), header.desc.range, MerkleTree.RECOMMENDED_DEPTH, (int)Math.pow(2, 15));
                Validator validator = new Validator(header.desc, message.from, tree);
                CompactionManager.instance.submitValidation(store, validator);
                break;

            case SYNC_REQUEST:
                // forwarded sync request
                SyncRequest request = (SyncRequest) message.payload.body;
                StreamingRepairTask task = new StreamingRepairTask(header.desc, request);
                task.run();
                break;

            default:
                ActiveRepairService.instance.handleMessage(message.from, message.payload);
                break;
        }
    }
}

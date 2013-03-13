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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.ActiveRepairService;

/**
 * Handler for requests from remote nodes to generate a valid tree.
 */
@Deprecated
public class TreeRequestVerbHandler implements IVerbHandler<TreeRequest>
{
    private static final Logger logger = LoggerFactory.getLogger(ActiveRepairService.class);

    /**
     * Trigger a validation compaction which will return the tree upon completion.
     */
    public void doVerb(MessageIn<TreeRequest> message, int id)
    {
        TreeRequest request = new TreeRequest(message.payload.desc, message.from);

        // trigger read-only compaction
        ColumnFamilyStore store = Table.open(request.desc.keyspace).getColumnFamilyStore(request.desc.columnFamily);
        Validator validator = new Validator(request);
        logger.debug("Queueing validation compaction for " + request);
        CompactionManager.instance.submitValidation(store, validator);
    }
}

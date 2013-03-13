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

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Handles streaming repair request from the older version.
 */
@Deprecated
public class StreamingRepairRequestVerbHandler implements IVerbHandler<StreamingRepairTask>
{
    private static Logger logger = LoggerFactory.getLogger(ActiveRepairService.class);

    public void doVerb(MessageIn<StreamingRepairTask> message, int id)
    {
        StreamingRepairTask task = message.payload;
        logger.info(String.format("[repair #%s] Received task %s from %s", task.desc.sessionId, task, message.from));
        task.run();
    }
}

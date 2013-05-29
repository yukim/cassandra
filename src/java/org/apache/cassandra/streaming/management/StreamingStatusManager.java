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
package org.apache.cassandra.streaming.management;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StreamingStatusManager tracks StreamEvent and maintains
 * streaming status in memory.
 */
public class StreamingStatusManager implements StreamEventHandler
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingStatusManager.class);

    private ConcurrentLinkedHashMap<String, OperationInfo> operations;

    public StreamingStatusManager()
    {
        this.operations = new ConcurrentLinkedHashMap.Builder<String, OperationInfo>()
                                .maximumWeightedCapacity(14)
                                .build();
    }

    public void handleStreamEvent(StreamEvent event)
    {
        logger.info(event.toString());
        switch (event.eventType)
        {
            case SESSION_PREPARED:
                OperationInfo op = ((StreamEvent.SessionPreparedEvent) event).operationInfo;
                if (!operations.containsKey(op.operationId))
                    operations.put(op.operationId, op);
                SessionInfo session = ((StreamEvent.SessionPreparedEvent) event).session;
                operations.get(op.operationId).sessions.add(session);
                break;
            case FILE_PROGRESS:
                break;
        }
    }
}

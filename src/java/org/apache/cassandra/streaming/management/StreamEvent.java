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

import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamSession;

public abstract class StreamEvent
{
    public enum Type
    {
        OPERATION_COMPLETE,
        SESSION_PREPARED,
        SESSION_COMPLETE,
        FILE_PROGRESS,
    }

    public final Type eventType;
    public final OperationInfo operationInfo;

    protected StreamEvent(Type eventType, StreamOperation operation)
    {
        this.eventType = eventType;
        this.operationInfo = new OperationInfo(operation);
    }

    public static class OperationCompleteEvent extends StreamEvent
    {
        public OperationCompleteEvent(StreamOperation operation)
        {
            super(Type.OPERATION_COMPLETE, operation);
        }
    }

    public static class SessionCompleteEvent extends StreamEvent
    {
        public final SessionInfo session;
        public final boolean success;

        public SessionCompleteEvent(StreamSession session)
        {
            super(Type.SESSION_COMPLETE, session.operation);
            this.session = new SessionInfo(session);
            this.success = session.isSuccess();
        }
    }

    public static class ProgressEvent extends StreamEvent
    {
        public final ProgressInfo progress;

        public ProgressEvent(StreamSession session, String fileName, byte direction, long currentBytes, long totalBytes)
        {
            super(Type.FILE_PROGRESS, session.operation);
            this.progress = new ProgressInfo(session.peer, fileName, direction, currentBytes, totalBytes);
        }

        @Override
        public String toString()
        {
            return "<ProgressEvent " + progress.toString() + ">";
        }
    }

    public static class SessionPreparedEvent extends StreamEvent
    {
        public final SessionInfo session;

        public SessionPreparedEvent(StreamSession session)
        {
            super(Type.SESSION_PREPARED, session.operation);
            this.session = new SessionInfo(session);
        }
    }
}

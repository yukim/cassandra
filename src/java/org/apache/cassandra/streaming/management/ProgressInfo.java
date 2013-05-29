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

import java.net.InetAddress;

/**
 * ProgressInfo contains file transfer progress.
 */
public class ProgressInfo
{
    public final InetAddress peer;
    public final String fileName;
    public final byte direction; /* 0:out, 1:in */
    public final long currentBytes;
    public final long totalBytes;

    public ProgressInfo(InetAddress peer, String fileName, byte direction, long currentBytes, long totalBytes)
    {
        this.peer = peer;
        this.fileName = fileName;
        this.direction = direction;
        this.currentBytes = currentBytes;
        this.totalBytes = totalBytes;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder(fileName);
        sb.append(" ").append(currentBytes);
        sb.append(" of ").append(totalBytes).append(" bytes ");
        sb.append(direction == 0 ? "sent to " : "received from ");
        sb.append(peer);
        return sb.toString();
    }
}

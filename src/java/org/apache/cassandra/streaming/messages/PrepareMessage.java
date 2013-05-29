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
package org.apache.cassandra.streaming.messages;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class PrepareMessage extends StreamMessage
{
    public static StreamMessage from(ReadableByteChannel in) throws IOException
    {
        DataInput input = new DataInputStream(Channels.newInputStream(in));
        PrepareMessage message = new PrepareMessage();
        // requests
        int numRequests = input.readInt();
        for (int i = 0; i < numRequests; i++)
            message.requests.add(StreamRequest.serializer.deserialize(input));
        // summaries
        int numSummaries = input.readInt();
        for (int i = 0; i < numSummaries; i++)
            message.summaries.add(StreamSummary.serializer.deserialize(input));
        return message;
    }

    /**
     * Streaming requests
     */
    public final Collection<StreamRequest> requests = new ArrayList<>();

    /**
     * Summaries of streaming out
     */
    public final Collection<StreamSummary> summaries = new ArrayList<>();

    public PrepareMessage()
    {
        super(Type.PREPARE);
    }

    @Override
    protected void writeMessage(WritableByteChannel out) throws IOException
    {
        DataOutput output = new DataOutputStream(Channels.newOutputStream(out));
        // requests
        output.writeInt(requests.size());
        for (StreamRequest request : requests)
            StreamRequest.serializer.serialize(request, output);
        // summaries
        output.writeInt(summaries.size());
        for (StreamSummary summary : summaries)
            StreamSummary.serializer.serialize(summary, output);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("PrepareMessage{");
        sb.append(requests.size()).append(" requests, ");
        int totalFile = 0;
        for (StreamSummary summary : summaries)
            totalFile += summary.files;
        sb.append(totalFile).append(" files receiving");
        sb.append('}');
        return sb.toString();
    }
}

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

import org.apache.cassandra.utils.CFPath;

/**
 */
public class RetryMessage extends StreamMessage
{
    public static StreamMessage from(ReadableByteChannel in) throws IOException
    {
        DataInput input = new DataInputStream(Channels.newInputStream(in));
        return new RetryMessage(new CFPath(input.readUTF(), input.readUTF()), input.readInt());
    }

    public final CFPath path;
    public final int sequenceNumber;

    public RetryMessage(CFPath path, int sequenceNumber)
    {
        super(Type.RETRY);
        this.path = path;
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    protected void writeMessage(WritableByteChannel out) throws IOException
    {
        DataOutput output = new DataOutputStream(Channels.newOutputStream(out));
        output.writeUTF(path.keyspace());
        output.writeUTF(path.columnFamily());
        output.writeInt(sequenceNumber);
    }
}

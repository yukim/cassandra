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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 */
public abstract class StreamMessage
{
    public static StreamMessage deserialize(ReadableByteChannel in) throws IOException
    {
        ByteBuffer buff = ByteBuffer.allocate(1);
        in.read(buff);
        buff.flip();
        byte type = buff.get();
        switch (Type.get(type))
        {
            case PREPARE:
                return PrepareMessage.from(in);
            case FILE:
                return FileMessage.from(in);
            case RETRY:
                return RetryMessage.from(in);
            case COMPLETE:
                return new CompleteMessage();
            case SESSION_FAILED:
                return new SessionFailedMessage();
            default:
                throw new IOException("no message defined for type " + type);
        }
    }

    public static enum Type
    {
        PREPARE(1, 5),
        FILE(2, 0),
        RETRY(3, 1),
        COMPLETE(4, 4),
        SESSION_FAILED(5, 5);

        public static Type get(byte type)
        {
            for (Type t : Type.values())
            {
                if (t.type == type)
                    return t;
            }
            throw new IllegalArgumentException("Unknown type " + type);
        }

        private final byte type;
        public final int priority;

        private Type(int type, int priority)
        {
            this.type = (byte) type;
            this.priority = priority;
        }
    }

    public final Type type;

    protected StreamMessage(Type type)
    {
        this.type = type;
    }

    public void write(WritableByteChannel out) throws IOException
    {
        // message type
        ByteBuffer buff = ByteBuffer.allocate(1);
        buff.put(type.type);
        buff.flip();
        out.write(buff);
        writeMessage(out);
    }

    /**
     * @return priority of this message. higher value, higher priority.
     */
    public int getPriority()
    {
        return type.priority;
    }

    /**
     * Implement message specific
     */
    protected abstract void writeMessage(WritableByteChannel out) throws IOException;
}

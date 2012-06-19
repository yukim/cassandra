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
package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * DirectBufferPool pre-allocates specified amount of memory off heap
 */
public final class DirectBufferPool
{
    private final int blockSize;
    private final ByteBuffer preallocated;
    private final BlockingQueue<ByteBuffer> pool;

    /**
     *
     */
    public DirectBufferPool(int blockSize, int count)
    {
        this.blockSize = blockSize;
        preallocated = ByteBuffer.allocateDirect(blockSize * count);
        pool = new LinkedBlockingQueue<ByteBuffer>(count);

        for (int i = 0; i < count; i++)
        {
            preallocated.position(i * blockSize);
            preallocated.limit((i + 1) * blockSize);
            pool.add(preallocated.slice());
        }
    }

    /**
     *
     * @param size allocation size
     * @return array of ByteBuffer to fulfill the requested size
     */
    public ByteBuffer[] allocate(int size)
    {
        assert size >= 0 && size < preallocated.capacity();

        if (size == 0)
            return new ByteBuffer[]{ByteBufferUtil.EMPTY_BYTE_BUFFER};

        try
        {
            int remainder = size % blockSize;
            int numBlocks = remainder == 0 ? size / blockSize : size / blockSize + 1;
            ByteBuffer[] buffers = new ByteBuffer[numBlocks];
            for (int i = 0; i < numBlocks; i++)
            {
                buffers[i] = pool.take();
                if (i == numBlocks - 1 && remainder > 0)
                {
                    buffers[i].limit(remainder);
                    buffers[i] = buffers[i].slice();
                }
                buffers[i].clear();
            }
            return buffers;
        }
        catch (InterruptedException e)
        {
            throw FBUtilities.unchecked(e);
        }
    }

    public int getBlockSize()
    {
        return blockSize;
    }

    /**
     *
     * @param buffers
     */
    public void free(ByteBuffer... buffers)
    {
        Collections.addAll(pool, buffers);
    }
}

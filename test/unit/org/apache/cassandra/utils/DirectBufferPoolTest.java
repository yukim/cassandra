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

import org.junit.Test;

/**
 * TODO class comment
 */
public class DirectBufferPoolTest
{
    @Test
    public void testAllocatingZero() throws Exception
    {
        DirectBufferPool pool = new DirectBufferPool(64, 3);
        ByteBuffer[] buffers = pool.allocate(0);
        assert buffers.length == 1;
        assert ByteBufferUtil.EMPTY_BYTE_BUFFER.equals(buffers[0]);
    }

    /**
     * requesting buffer greater than pre-allocated causes AssertionError
     */
    @Test(expected = AssertionError.class)
    public void testAllocatingOffLimit() throws Exception
    {
        DirectBufferPool pool = new DirectBufferPool(64, 3);
        pool.allocate(64 * 3 + 1);
    }

    @Test
    public void testAllocatingLessThanBlockSize() throws Exception
    {
        DirectBufferPool pool = new DirectBufferPool(64, 3);
        ByteBuffer[] buffers = pool.allocate(32);
        assert buffers.length == 1;
        assert buffers[0].position() == 0;
        assert buffers[0].limit() == 32;
        assert buffers[0].capacity() == 32;
    }

    @Test
    public void testAllocatingEqualToBlockSize() throws Exception
    {
        DirectBufferPool pool = new DirectBufferPool(64, 3);
        ByteBuffer[] buffers = pool.allocate(64);
        assert buffers.length == 1;
        assert buffers[0].position() == 0;
        assert buffers[0].limit() == 64;
        assert buffers[0].capacity() == 64;
    }

    @Test
    public void testAllocatingLargerThanBlockSize() throws Exception
    {
        DirectBufferPool pool = new DirectBufferPool(64, 3);
        ByteBuffer[] buffers = pool.allocate(96);
        assert buffers.length == 2;

        assert buffers[0].position() == 0;
        assert buffers[0].limit() == 64;
        assert buffers[0].capacity() == 64;

        assert buffers[1].position() == 0;
        assert buffers[1].limit() == 32;
        assert buffers[1].capacity() == 32;
    }

    @Test
    public void testRequestingOverCount() throws Exception
    {
        final DirectBufferPool pool = new DirectBufferPool(64, 3);

        // should be able to perform allocation if more than the number of block is
        // accessed when allocated block is properly freed.
        int numThreads = 8;
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++)
        {
            Thread t = new Thread(new Runnable()
            {
                public void run()
                {
                    ByteBuffer[] buffers = pool.allocate(64);
                    assert buffers.length == 1;
                    assert buffers[0].position() == 0;
                    assert buffers[0].limit() == 64;
                    assert buffers[0].capacity() == 64;
                    pool.free(buffers);
                }
            });
            threads[i] = t;
            t.start();
        }
        for (Thread t : threads)
            t.join();
    }
}

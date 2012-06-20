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
import java.util.Random;

import org.junit.Test;
import org.xerial.snappy.Snappy;

/**
 * TODO class comment
 */
public class SnappyTest
{
    private Random r = new Random(System.nanoTime());

    @Test
    public void testBB() throws Exception
    {
        for (int c = 0; c < 10; c++)
        {
            ByteBuffer bb = ByteBuffer.allocateDirect(64 * 1024);
            bb.limit(28 * 1024);
            ByteBuffer src = bb.slice();
            bb.position(28 * 1024).limit(bb.capacity());
            ByteBuffer dst = bb.slice();

            for (int i = 0; i < src.capacity(); i++)
                src.put(i, randomeByte());

            long start = System.nanoTime();
            Snappy.compress(src, dst);
            System.out.printf("%d) BB: %d nsec\n", c, System.nanoTime() - start);
        }
    }

    @Test
    public void testBArray() throws Exception
    {
        for (int c = 0; c < 10; c++)
        {
            byte[] src = new byte[28 * 1204];
            byte[] dst = new byte[36 * 1024];
            for (int i = 0; i < src.length; i++)
                src[i] = randomeByte();

            long start = System.nanoTime();
            Snappy.compress(src, 0, src.length, dst, 0);
            System.out.printf("%d) b[]: %d nsec\n", c, System.nanoTime() - start);
        }
    }

    private byte randomeByte()
    {
        return (byte) r.nextInt(255);
    }
}

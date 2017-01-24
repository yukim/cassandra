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

import java.io.IOException;
import java.net.InetAddress;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class RepairSessionTest
{
    @Test
    public void testConviction() throws Exception
    {
        InetAddress remote = InetAddress.getByName("127.0.0.2");
        Gossiper.instance.initializeNodeUnsafe(remote, UUID.randomUUID(), 1);

        // Set up RepairSession
        UUID parentSessionId = UUIDGen.getTimeUUID();
        UUID sessionId = UUID.randomUUID();
        IPartitioner p = Murmur3Partitioner.instance;
        Range<Token> repairRange = new Range<>(p.getToken(ByteBufferUtil.bytes(0)), p.getToken(ByteBufferUtil.bytes(100)));
        Set<InetAddress> endpoints = Sets.newHashSet(remote);

        // Test queue that takes time to poll so that thread won't be created
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>()
        {
            public Runnable poll(long timeout, TimeUnit unit) throws InterruptedException
            {
                Thread.sleep(unit.toMillis(timeout));
                return super.poll(timeout, unit);
            }
        };
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, Long.MAX_VALUE, TimeUnit.MINUTES, queue);

        String[] repairJobs = {"Standard1", "Standard2", "Standard3", "Standard4"};
        RepairSession session = ActiveRepairService.instance.submitRepairSession(parentSessionId,
                                                                                 repairRange,
                                                                                 "Keyspace1",
                                                                                 RepairParallelism.SEQUENTIAL,
                                                                                 endpoints,
                                                                                 0,
                                                                                 executor,
                                                                                 repairJobs);

        assertEquals(repairJobs.length - 1, queue.size());
        // perform convict
        session.convict(remote, Double.MAX_VALUE);

        // RepairSession should throw ExecutorException with the cause of IOException when getting its value
        try
        {
            session.get();
            fail();
        }
        catch (ExecutionException ex)
        {
            // Make sure forced termination clears repair job
            assertEquals(0, queue.size());
            assertSame(IOException.class, ex.getCause().getClass());
        }
    }
}

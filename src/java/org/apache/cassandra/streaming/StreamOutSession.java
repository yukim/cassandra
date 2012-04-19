/**
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

package org.apache.cassandra.streaming;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;

/**
 * This class manages the streaming of multiple files one after the other.
 */
public class StreamOutSession extends AbstractStreamSession
{
    private static final Logger logger = LoggerFactory.getLogger(StreamOutSession.class);

    // one host may have multiple stream sessions.
    private static final ConcurrentMap<Pair<InetAddress, Long>, StreamOutSession> streams = new NonBlockingHashMap<Pair<InetAddress, Long>, StreamOutSession>();

    public static StreamOutSession create(String table, InetAddress host, IStreamCallback callback)
    {
        return create(table, host, System.nanoTime(), callback);
    }

    public static StreamOutSession create(String table, InetAddress host, long sessionId)
    {
        return create(table, host, sessionId, null);
    }

    public static StreamOutSession create(String table, InetAddress host, long sessionId, IStreamCallback callback)
    {
        return create(table, host, sessionId, callback, 1);
    }

    public static StreamOutSession create(String table, InetAddress host, long sessionId, IStreamCallback callback, int maxConcurrency)
    {
        Pair<InetAddress, Long> context = new Pair<InetAddress, Long>(host, sessionId);
        StreamOutSession session = new StreamOutSession(table, context, callback, maxConcurrency);
        streams.put(context, session);
        return session;
    }

    public static StreamOutSession get(InetAddress host, long sessionId)
    {
        return streams.get(new Pair<InetAddress, Long>(host, sessionId));
    }

    // active pending files keyed by file name
    private final Map<String, PendingFile> active = new NonBlockingHashMap<String, PendingFile>();
    private final Queue<PendingFile> pending = new ConcurrentLinkedQueue<PendingFile>();

    private final int maxConcurrency;

    private StreamOutSession(String table, Pair<InetAddress, Long> context, IStreamCallback callback, int maxConcurrency)
    {
        super(table, context, callback);
        this.maxConcurrency = maxConcurrency;
    }

    public void addFilesToStream(List<PendingFile> pendingFiles)
    {
        for (PendingFile pendingFile : pendingFiles)
        {
            if (logger.isDebugEnabled())
                logger.debug("Adding file {} to be streamed.", pendingFile.getFilename());
            pending.add(pendingFile);
        }
    }

    public void retry(String file)
    {
        assert active.containsKey(file);
        streamFile(active.get(file));
    }

    private void streamFile(PendingFile pf)
    {
        if (logger.isDebugEnabled())
            logger.debug("Streaming {} ...", pf);
        active.put(pf.getFilename(), pf);
        MessagingService.instance().stream(new StreamHeader(table, getSessionId(), pf), getHost());
    }

    public void finishAndStartNext(String finished) throws IOException
    {
        assert active.containsKey(finished);
        active.get(finished).sstable.releaseReference();
        active.remove(finished);
        if (!pending.isEmpty())
            streamFile(pending.poll());
    }

    protected void closeInternal(boolean success)
    {
        // Release reference on last file (or any uncompleted ones)
        for (PendingFile file : active.values())
            file.sstable.releaseReference();
        streams.remove(context);
    }

    /** convenience method for use when testing */
    void await() throws InterruptedException
    {
        while (streams.containsKey(context))
            Thread.sleep(10);
    }

    public Collection<PendingFile> getFiles()
    {
        Set<PendingFile> files = new HashSet<PendingFile>(pending);
        files.addAll(active.values());
        return files;
    }

    public static Set<InetAddress> getDestinations()
    {
        Set<InetAddress> hosts = new HashSet<InetAddress>();
        for (StreamOutSession session : streams.values())
            hosts.add(session.getHost());
        return hosts;
    }

    public static List<PendingFile> getOutgoingFiles(InetAddress host)
    {
        List<PendingFile> list = new ArrayList<PendingFile>();
        for (Map.Entry<Pair<InetAddress, Long>, StreamOutSession> entry : streams.entrySet())
        {
            if (entry.getKey().left.equals(host))
                list.addAll(entry.getValue().getFiles());
        }
        return list;
    }

    public void validateCurrentFile(String file)
    {
        if (!active.containsKey(file))
            throw new IllegalStateException(String.format("target reported current file %s is not in %s", file, active.keySet()));
    }

    /**
     * Begins streaming session with maximum concurrency specified in constructor.
     * Note that concurrency is actually defined in MessagingService's streamExecutor.
     */
    public void begin()
    {
        logger.info("Streaming to {}", getHost());
        //logger.debug("Files are {}", StringUtils.join(active.values(), ","));

        // send first streaming with all pending files
        PendingFile pf = pending.poll();
        if (logger.isDebugEnabled())
            logger.debug("Streaming {} ...", pf);
        active.put(pf.getFilename(), pf);
        StreamHeader header = new StreamHeader(table, getSessionId(), pf, pending);
        MessagingService.instance().stream(header, getHost());

        // and streams rest til hit max concurrency
        int maxIteration = Math.min(maxConcurrency, pending.size());
        for (int i = 1; i < maxIteration; i++)
            streamFile(pending.poll());
    }
}

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
package org.apache.cassandra.io.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.CLibrary;
import org.apache.cassandra.utils.DirectBufferPool;

public class SequentialWriter extends OutputStream
{
    protected static final DirectBufferPool pool = new DirectBufferPool(RandomAccessReader.DEFAULT_BUFFER_SIZE, 1024);

    // isDirty - true if this.buffer contains any un-synced bytes
    protected boolean isDirty = false, syncNeeded = false;

    // absolute path to the given file
    private final String filePath;

    // so we can use the write(int) path w/o tons of new byte[] allocations
    private final byte[] singleByteBuffer = new byte[1];

    protected ByteBuffer[] buffers;
    protected int bufferCapacity;

    private final boolean skipIOCache;
    private final int fd;
    private final int directoryFD;
    // directory should be synced only after first file sync, in other words, only once per file
    private boolean directorySynced = false;

    protected long current = 0, bufferOffset;
    protected int validBufferBytes;

    protected final RandomAccessFile out;

    // used if skip I/O cache was enabled
    private long ioCacheStartOffset = 0, bytesSinceCacheFlush = 0;

    // whether to do trickling fsync() to avoid sudden bursts of dirty buffer flushing by kernel causing read
    // latency spikes
    private boolean trickleFsync;
    private int trickleFsyncByteInterval;
    private int bytesSinceTrickleFsync = 0;

    public final DataOutputStream stream;
    private MessageDigest digest;

    public SequentialWriter(File file, int bufferSize, boolean skipIOCache) throws IOException
    {
        out = new RandomAccessFile(file, "rw");

        filePath = file.getAbsolutePath();

        buffers = allocate(bufferSize);
        for (ByteBuffer buff : buffers)
            bufferCapacity += buff.capacity();

        this.skipIOCache = skipIOCache;
        this.trickleFsync = DatabaseDescriptor.getTrickleFsync();
        this.trickleFsyncByteInterval = DatabaseDescriptor.getTrickleFsyncIntervalInKb() * 1024;
        fd = CLibrary.getfd(out.getFD());
        directoryFD = CLibrary.tryOpenDirectory(file.getParent());
        stream = new DataOutputStream(this);
    }

    public static SequentialWriter open(File file) throws IOException
    {
        return open(file, RandomAccessReader.DEFAULT_BUFFER_SIZE, false);
    }

    public static SequentialWriter open(File file, boolean skipIOCache) throws IOException
    {
        return open(file, RandomAccessReader.DEFAULT_BUFFER_SIZE, skipIOCache);
    }

    public static SequentialWriter open(File file, int bufferSize, boolean skipIOCache) throws IOException
    {
        return new SequentialWriter(file, bufferSize, skipIOCache);
    }

    public void write(int value) throws IOException
    {
        singleByteBuffer[0] = (byte) value;
        write(singleByteBuffer, 0, 1);
    }

    public void write(byte[] buffer) throws IOException
    {
        write(buffer, 0, buffer.length);
    }

    public void write(byte[] data, int offset, int length) throws IOException
    {
        if (buffers == null)
            throw new ClosedChannelException();

        while (length > 0)
        {
            int n = writeAtMost(data, offset, length);
            offset += n;
            length -= n;
            isDirty = true;
            syncNeeded = true;
        }
    }

    /*
     * Write at most "length" bytes from "b" starting at position "offset", and
     * return the number of bytes written. caller is responsible for setting
     * isDirty.
     */
    private int writeAtMost(byte[] data, int offset, int length) throws IOException
    {
        if (current >= bufferOffset + bufferCapacity)
            reBuffer();

        assert current < bufferOffset + bufferCapacity
                : String.format("File (%s) offset %d, buffer offset %d.", getPath(), current, bufferOffset);


        int toCopy = Math.min(length, bufferCapacity - bufferCursor());

        // copy bytes from external buffer
        int copied = 0;
        for (ByteBuffer buff : buffers)
        {
            while (buff.hasRemaining() && copied < toCopy)
            {
                buff.put(data[offset + copied]);
                copied++;
            }
        }

        assert current <= bufferOffset + bufferCapacity
                : String.format("File (%s) offset %d, buffer offset %d.", getPath(), current, bufferOffset);

        validBufferBytes = Math.max(validBufferBytes, bufferCursor() + toCopy);
        current += toCopy;

        return toCopy;
    }

    /**
     * Synchronize file contents with disk.
     * @throws java.io.IOException on any I/O error.
     */
    public void sync() throws IOException
    {
        syncInternal();
    }

    protected void syncDataOnlyInternal() throws IOException
    {
        out.getFD().sync();
    }

    protected void syncInternal() throws IOException
    {
        if (syncNeeded)
        {
            flushInternal();
            syncDataOnlyInternal();

            if (!directorySynced)
            {
                CLibrary.trySync(directoryFD);
                directorySynced = true;
            }

            syncNeeded = false;
        }
    }

    /**
     * If buffer is dirty, flush it's contents to the operating system. Does not imply fsync().
     *
     * Currently, for implementation reasons, this also invalidates the buffer.
     *
     * @throws java.io.IOException on any I/O error.
     */
    @Override
    public void flush() throws IOException
    {
        flushInternal();
    }

    protected void flushInternal() throws IOException
    {
        if (isDirty)
        {
            flushData();

            if (trickleFsync)
            {
                bytesSinceTrickleFsync += validBufferBytes;
                if (bytesSinceTrickleFsync >= trickleFsyncByteInterval)
                {
                    syncDataOnlyInternal();
                    bytesSinceTrickleFsync = 0;
                }
            }

            if (skipIOCache)
            {
                // we don't know when the data reaches disk since we aren't
                // calling flush
                // so we continue to clear pages we don't need from the first
                // offset we see
                // periodically we update this starting offset
                bytesSinceCacheFlush += validBufferBytes;

                if (bytesSinceCacheFlush >= RandomAccessReader.CACHE_FLUSH_INTERVAL_IN_BYTES)
                {
                    CLibrary.trySkipCache(this.fd, ioCacheStartOffset, 0);
                    ioCacheStartOffset = bufferOffset;
                    bytesSinceCacheFlush = 0;
                }
            }

            // Remember that we wrote, so we don't write it again on next flush().
            resetBuffer();

            isDirty = false;
        }
    }

    /**
     * Override this method instead of overriding flush()
     * @throws IOException on any I/O error.
     */
    protected void flushData() throws IOException
    {
        for (ByteBuffer buff : buffers)
            buff.flip();
        out.getChannel().write(buffers);
        if (digest != null)
        {
            for (ByteBuffer buf : buffers)
            {
                buf.flip();
                digest.update(buf);
            }
        }
    }

    public long getFilePointer()
    {
        return current;
    }

    public long length() throws IOException
    {
        return Math.max(Math.max(current, out.length()), bufferOffset + validBufferBytes);
    }

    public String getPath()
    {
        return filePath;
    }

    protected void reBuffer() throws IOException
    {
        flushInternal();
        resetBuffer();
    }

    protected void resetBuffer()
    {
        bufferOffset = current;
        validBufferBytes = 0;
        for (ByteBuffer buff : buffers)
            buff.clear();
    }

    private int bufferCursor()
    {
        return (int) (current - bufferOffset);
    }

    public FileMark mark()
    {
        return new BufferedFileWriterMark(current);
    }

    public void resetAndTruncate(FileMark mark) throws IOException
    {
        assert mark instanceof BufferedFileWriterMark;

        long previous = current;
        current = ((BufferedFileWriterMark) mark).pointer;

        if (previous - current <= validBufferBytes) // current buffer
        {
            validBufferBytes = validBufferBytes - ((int) (previous - current));
            buffers[validBufferBytes / pool.getBlockSize()].position(validBufferBytes % pool.getBlockSize());
            return;
        }

        // synchronize current buffer with disk
        // because we don't want any data loss
        syncInternal();

        // truncate file to given position
        truncate(current);

        // reset channel position
        out.seek(current);

        resetBuffer();
    }

    public void truncate(long toSize) throws IOException
    {
        out.getChannel().truncate(toSize);
    }

    @Override
    public void close() throws IOException
    {
        if (buffers == null)
            return; // already closed

        syncInternal();

        pool.free(buffers);
        buffers = null;

        if (skipIOCache && bytesSinceCacheFlush > 0)
            CLibrary.trySkipCache(fd, 0, 0);

        out.close();
        CLibrary.tryCloseFD(directoryFD);
    }

    /**
     * Turn on digest computation on this writer.
     * This can only be called before any data is written to this write,
     * otherwise an IllegalStateException is thrown.
     */
    public void setComputeDigest()
    {
        if (current != 0)
            throw new IllegalStateException();

        try
        {
            digest = MessageDigest.getInstance("SHA-1");
        }
        catch (NoSuchAlgorithmException e)
        {
            // SHA-1 is standard in java 6
            throw new RuntimeException(e);
        }
    }

    /**
     * Return the digest associated to this file or null if no digest was
     * created.
     * This can only be called once the file is fully created, i.e. after
     * close() has been called. Otherwise an IllegalStateException is thrown.
     */
    public byte[] digest()
    {
        if (buffers != null)
            throw new IllegalStateException();

        return digest == null ? null : digest.digest();
    }

    protected ByteBuffer[] allocate(int size)
    {
        return pool.allocate(size);
    }

    /**
     * Class to hold a mark to the position of the file
     */
    protected static class BufferedFileWriterMark implements FileMark
    {
        final long pointer;

        public BufferedFileWriterMark(long pointer)
        {
            this.pointer = pointer;
        }
    }
}

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

import java.util.Optional;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.utils.CLibrary;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;

import static org.apache.cassandra.utils.Throwables.maybeFail;

/**
 * Abstracts a read-only file that has been split into segments, each of which can be represented by an independent
 * FileDataInput. Allows for iteration over the FileDataInputs, or random access to the FileDataInput for a given
 * position.
 *
 * The JVM can only map up to 2GB at a time, so each segment is at most that size when using mmap i/o. If a segment
 * would need to be longer than 2GB, that segment will not be mmap'd, and a new RandomAccessFile will be created for
 * each access to that segment.
 */
public class SegmentedFile extends SharedCloseableImpl
{
    private static final Logger logger = LoggerFactory.getLogger(SegmentedFile.class);

    public final ChannelProxy channel;

    // This differs from length for compressed files (but we still need length for
    // SegmentIterator because offsets in the file are relative to the uncompressed size)
    public final long onDiskLength;

    /**
     * Rebufferer to use to construct RandomAccessReaders.
     */
    private final RebuffererFactory rebufferer;

    /**
     * Optional CompressionMetadata when dealing with compressed file
     */
    private final Optional<CompressionMetadata> compressionMetadata;

    protected SegmentedFile(Cleanup cleanup,
                            ChannelProxy channel,
                            RebuffererFactory rebufferer,
                            CompressionMetadata compressionMetadata,
                            long onDiskLength)
    {
        super(cleanup);
        this.rebufferer = rebufferer;
        this.channel = channel;
        this.compressionMetadata = Optional.ofNullable(compressionMetadata);
        this.onDiskLength = onDiskLength;
    }

    protected SegmentedFile(SegmentedFile copy)
    {
        super(copy);
        channel = copy.channel;
        rebufferer = copy.rebufferer;
        compressionMetadata = copy.compressionMetadata;
        onDiskLength = copy.onDiskLength;
    }

    public String path()
    {
        return channel.filePath();
    }

    public long dataLength()
    {
        return compressionMetadata.map(c -> c.dataLength).orElseGet(rebufferer::fileLength);
    }

    public RebuffererFactory rebuffererFactory()
    {
        return rebufferer;
    }

    public Optional<CompressionMetadata> compressionMetadata()
    {
        return compressionMetadata;
    }

    @Override
    public void addTo(Ref.IdentityCollection identities)
    {
        super.addTo(identities);
        compressionMetadata.ifPresent(metadata -> metadata.addTo(identities));
    }

    protected static class Cleanup implements RefCounted.Tidy
    {
        final ChannelProxy channel;
        final RebuffererFactory rebufferer;
        final CompressionMetadata metadata;
        final Optional<ChunkCache> chunkCache;

        public Cleanup(ChannelProxy channel,
                       RebuffererFactory rebufferer,
                       CompressionMetadata metadata,
                       ChunkCache chunkCache)
        {
            this.channel = channel;
            this.rebufferer = rebufferer;
            this.metadata = metadata;
            this.chunkCache = Optional.ofNullable(chunkCache);
        }

        public String name()
        {
            return channel.filePath();
        }

        public void tidy()
        {
            if (metadata != null)
            {
                chunkCache.ifPresent(cache -> cache.invalidateFile(name()));
                metadata.close();
            }
            try
            {
                channel.close();
            }
            finally
            {
                rebufferer.close();
            }
        }
    }

    public SegmentedFile sharedCopy()
    {
        return new SegmentedFile(this);
    }

    public RandomAccessReader createReader()
    {
        return RandomAccessReader.build(this, null);
    }

    public RandomAccessReader createReader(RateLimiter limiter)
    {
        return RandomAccessReader.build(this, limiter);
    }

    public FileDataInput createReader(long position)
    {
        RandomAccessReader reader = createReader();
        reader.seek(position);
        return reader;
    }

    /**
     * Drop page cache from start to given {@code before}.
     *
     * @param before uncompressed position from start of the file to be dropped from cache. if 0, to end of file.
     */
    public void dropPageCache(long before)
    {
        long position = compressionMetadata.map(metadata -> {
            if (before >= metadata.dataLength)
                return 0L;
            else
                return metadata.chunkFor(before).offset;
        }).orElse(before);
        CLibrary.trySkipCache(channel.getFileDescriptor(), 0, position, path());
    }

    /**
     * Collects potential segmentation points in an underlying file, and builds a SegmentedFile to represent it.
     */
    public static class Builder implements AutoCloseable
    {
        private ChannelProxy channel;
        private CompressionMetadata compressionMetadata;
        private MmappedRegions regions;
        private ChunkCache chunkCache;

        private boolean mmapped = false;
        private boolean compressed = false;

        public Builder compressed(boolean compressed)
        {
            this.compressed = compressed;
            return this;
        }

        public Builder withChunkCache(ChunkCache chunkCache)
        {
            this.chunkCache = chunkCache;
            return this;
        }

        public Builder withCompressionMetadata(CompressionMetadata metadata)
        {
            this.compressionMetadata = metadata;
            return this;
        }

        public Builder mmapped(boolean mmapped)
        {
            this.mmapped = mmapped;
            return this;
        }

        /**
         * Called after all potential boundaries have been added to apply this Builder to a concrete file on disk.
         * @param channel The channel to the file on disk.
         */
        protected SegmentedFile complete(ChannelProxy channel, int bufferSize, long overrideLength)
        {
            if (compressed && compressionMetadata == null)
                compressionMetadata = CompressionMetadata.create(channel.filePath());

            long length = overrideLength > 0 ? overrideLength : compressed ? compressionMetadata.compressedFileLength : channel.size();

            RebuffererFactory rebuffererFactory;
            if (mmapped)
            {
                if (compressed)
                {
                    regions = MmappedRegions.map(channel, compressionMetadata);
                    rebuffererFactory = maybeCached(new CompressedChunkReader.Mmap(channel, compressionMetadata,
                                                                                   regions));
                }
                else
                {
                    updateRegions(channel, length);
                    rebuffererFactory = new MmapRebufferer(channel, length, regions.sharedCopy());
                }
            }
            else
            {
                regions = null;
                if (compressed)
                {
                    rebuffererFactory = maybeCached(new CompressedChunkReader.Standard(channel, compressionMetadata));
                }
                else
                {
                    rebuffererFactory = maybeCached(new SimpleChunkReader(channel, length, BufferType.OFF_HEAP,
                                                                          bufferSize));
                }
            }
            return new SegmentedFile(new Cleanup(channel, rebuffererFactory, compressionMetadata, chunkCache),
                                     channel, rebuffererFactory, compressionMetadata, length);
        }

        public SegmentedFile complete(String path, int bufferSize)
        {
            return complete(path, bufferSize, -1L);
        }

        @SuppressWarnings("resource") // SegmentedFile owns channel
        public SegmentedFile complete(String path, int bufferSize, long overrideLength)
        {
            ChannelProxy channelCopy = getChannel(path);
            try
            {
                return complete(channelCopy, bufferSize, overrideLength);
            }
            catch (Throwable t)
            {
                channelCopy.close();
                throw t;
            }
        }

        public Throwable close(Throwable accumulate)
        {
            if (!compressed && regions != null)
                accumulate = regions.close(accumulate);
            if (channel != null)
                return channel.close(accumulate);

            return accumulate;
        }

        public void close()
        {
            maybeFail(close(null));
        }

        private RebuffererFactory maybeCached(ChunkReader reader)
        {
            if (chunkCache != null && chunkCache.capacity() > 0)
                return chunkCache.wrap(reader);
            return reader;
        }

        private void updateRegions(ChannelProxy channel, long length)
        {
            if (regions != null && !regions.isValid(channel))
            {
                Throwable err = regions.close(null);
                if (err != null)
                    logger.error("Failed to close mapped regions", err);

                regions = null;
            }

            if (regions == null)
                regions = MmappedRegions.map(channel, length);
            else
                regions.extend(length);
        }

        private ChannelProxy getChannel(String path)
        {
            if (channel != null)
            {
                // This is really fragile, both path and channel.filePath()
                // must agree, i.e. they both must be absolute or both relative
                // eventually we should really pass the filePath to the builder
                // constructor and remove this
                if (channel.filePath().equals(path))
                    return channel.sharedCopy();
                else
                    channel.close();
            }

            channel = new ChannelProxy(path);
            return channel.sharedCopy();
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(path='" + path() + '\'' +
               ", length=" + rebufferer.fileLength() +
               ')';
    }
}

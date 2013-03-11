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
package org.apache.cassandra.cache;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

public class KeyCacheKey implements CacheKey
{
    /**
     * ColumnFamily ID.
     * Can be null if ColumnFamily does not exit on instantiation.
     */
    public final UUID cfId;

    /**
     * SSTable file generation number.
     */
    public final int generation;

    /**
     * true if SSTable has promoted index
     */
    public final boolean hasPromotedIndexes;

    // keeping an array instead of a ByteBuffer lowers the overhead of the key cache working set,
    // without extra copies on lookup since client-provided key ByteBuffers will be array-backed already
    public final byte[] key;

    public KeyCacheKey(Descriptor desc, ByteBuffer key)
    {
        this.cfId = Schema.instance.getId(desc.ksname, desc.cfname);
        this.generation = desc.generation;
        this.hasPromotedIndexes = desc.version.hasPromotedIndexes;
        this.key = ByteBufferUtil.getArray(key);
        assert this.key != null;
    }

    public Pair<String, String> getPathInfo()
    {
        return Schema.instance.getCF(cfId);
    }

    public String toString()
    {
        return String.format("KeyCacheKey(%s, %s)", cfId + "-" + generation, ByteBufferUtil.bytesToHex(ByteBuffer.wrap(key)));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyCacheKey that = (KeyCacheKey) o;

        if (cfId != null ? !cfId.equals(that.cfId) : that.cfId != null) return false;
        return generation == that.generation && Arrays.equals(key, that.key);
    }

    @Override
    public int hashCode()
    {
        int result = cfId != null ? cfId.hashCode() : 0;
        result = 31 * result + generation + (key != null ? Arrays.hashCode(key) : 0);
        return result;
    }
}

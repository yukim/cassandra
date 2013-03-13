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

import java.util.Collection;
import java.util.UUID;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

/**
 * RepairJobDesc is used from various repair processes to distinguish one RepairJob to another.
 *
 * @since 2.0
 */
public class RepairJobDesc
{
    /** RepairSession id */
    public final UUID sessionId;
    public final String keyspace;
    public final String columnFamily;
    /** repairing range  */
    public final Range<Token> range;

    public RepairJobDesc(UUID sessionId, String keyspace, String columnFamily, Range<Token> range)
    {
        this.sessionId = sessionId;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.range = range;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RepairJobDesc that = (RepairJobDesc) o;

        if (!columnFamily.equals(that.columnFamily)) return false;
        if (!keyspace.equals(that.keyspace)) return false;
        if (range != null ? !range.equals(that.range) : that.range != null) return false;
        if (!sessionId.equals(that.sessionId)) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = sessionId.hashCode();
        result = 31 * result + keyspace.hashCode();
        result = 31 * result + columnFamily.hashCode();
        result = 31 * result + (range != null ? range.hashCode() : 0);
        return result;
    }
}
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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * Repair preview kind
 */
public enum PreviewKind
{
    /**
     * No preview
     */
    NONE(0, null),
    /**
     * Full repair preview
     */
    FULL(1, Predicates.alwaysTrue()),
    /**
     * Incremental repair preview
     */
    INCREMENTAL(2, Predicates.not(SSTableReader::isRepaired)),
    /**
     * Repair validation
     */
    VALIDATE(3, SSTableReader::isRepaired);

    private final int serializationVal;
    private final Predicate<SSTableReader> validationPredicate;

    PreviewKind(int serializationVal, Predicate<SSTableReader> validationPredicate)
    {
        assert ordinal() == serializationVal;
        this.serializationVal = serializationVal;
        this.validationPredicate = validationPredicate;
    }

    public int getSerializationVal()
    {
        return serializationVal;
    }

    public static PreviewKind deserialize(int serializationVal)
    {
        return values()[serializationVal];
    }

    public Predicate<SSTableReader> getValidationPredicate()
    {
        return validationPredicate;
    }

    public boolean isPreview()
    {
        return this != NONE;
    }
}

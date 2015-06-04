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

package org.apache.cassandra.dht;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Partition splitter.
 */
public abstract class Splitter
{
    private final IPartitioner partitioner;

    protected Splitter(IPartitioner partitioner)
    {
        this.partitioner = partitioner;
    }

    protected abstract Token tokenForValue(BigInteger value);

    protected abstract BigInteger valueForToken(Token token);

    public List<Token> splitFullRange(int parts)
    {
        if (parts == 1)
            return Collections.singletonList(partitioner.getMaximumToken());
        return splitRange(partitioner.getMinimumToken(), partitioner.getMaximumToken(), parts);
    }

    public List<Token> splitRange(Token start, Token end, int parts)
    {
        if (parts == 1)
            return Collections.singletonList(partitioner.getMaximumToken());

        BigInteger startValue = valueForToken(start);
        BigInteger endValue = valueForToken(end.equals(partitioner.getMinimumToken()) ? partitioner.getMaximumToken() : end);
        List<BigInteger> boundaries = new ArrayList<>(parts);

        BigInteger partWidth = endValue.subtract(startValue).divide(BigInteger.valueOf(parts));
        boundaries.add(startValue.add(partWidth));

        for (int i = 1; i < parts - 1; i++)
            boundaries.add(boundaries.get(i - 1).add(partWidth));

        List<Token> tokenBoundaries = new ArrayList<>(parts);
        for (BigInteger boundary : boundaries)
            tokenBoundaries.add(tokenForValue(boundary));

        tokenBoundaries.add(partitioner.getMaximumToken());

        return tokenBoundaries;
    }
}

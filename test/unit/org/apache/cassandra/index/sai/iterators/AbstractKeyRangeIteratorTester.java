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
package org.apache.cassandra.index.sai.iterators;

import java.util.Arrays;
import java.util.function.BiFunction;

import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;

public class AbstractKeyRangeIteratorTester extends SAIRandomizedTester
{
    protected long[] arr(long... longArray)
    {
        return longArray;
    }

    protected long[] arr(int... intArray)
    {
        return Arrays.stream(intArray).mapToLong(i -> i).toArray();
    }

    final KeyRangeIterator buildIntersection(KeyRangeIterator... ranges)
    {
        return KeyRangeIntersectionIterator.builder(16, Integer.MAX_VALUE).add(Arrays.asList(ranges)).build();
    }

    final KeyRangeIterator buildSelectiveIntersection(int limit, KeyRangeIterator... ranges)
    {
        return KeyRangeIntersectionIterator.builder(16, limit).add(Arrays.asList(ranges)).build();
    }

    final KeyRangeIterator buildIntersection(long[]... ranges)
    {
        return buildIntersection(toRangeIterator(ranges));
    }

    final KeyRangeIterator buildSelectiveIntersection(int limit, long[]... ranges)
    {
        return buildSelectiveIntersection(limit, toRangeIterator(ranges));
    }

    final KeyRangeIterator buildUnion(KeyRangeIterator... ranges)
    {
        return KeyRangeUnionIterator.builder(ranges.length).add(Arrays.asList(ranges)).build();
    }

    final KeyRangeIterator buildUnion(long[]... ranges)
    {
        return buildUnion(toRangeIterator(ranges));
    }

    static KeyRangeIterator buildConcat(KeyRangeIterator... ranges)
    {
        return KeyRangeConcatIterator.builder(ranges.length).add(Arrays.asList(ranges)).build();
    }

    final KeyRangeIterator buildConcat(long[]... ranges)
    {
        return buildConcat(toRangeIterator(ranges));
    }

    private static KeyRangeIterator[] toRangeIterator(long[]... ranges)
    {
        return Arrays.stream(ranges).map(AbstractKeyRangeIteratorTester::build).toArray(KeyRangeIterator[]::new);
    }

    protected static LongIterator build(long... tokens)
    {
        return new LongIterator(tokens);
    }

    protected KeyRangeIterator build(BiFunction<KeyRangeIterator, KeyRangeIterator, KeyRangeIterator> builder,
                                     long[] tokensA,
                                     long[] tokensB)
    {
        return builder.apply(build(tokensA), build(tokensB));
    }
}

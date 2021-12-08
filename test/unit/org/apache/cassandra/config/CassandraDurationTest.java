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
package org.apache.cassandra.config;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.*;
import static org.quicktheories.QuickTheory.qt;

public class CassandraDurationTest
{
    @Test
    public void testConversions()
    {
        assertEquals(10, new CassandraDuration("10s").toSeconds());
        assertEquals(10000, new CassandraDuration("10s").toMilliseconds());
        assertEquals(0, new CassandraDuration("10s").toMinutes());
        assertEquals(10, new CassandraDuration("10m").toMinutes());
        assertEquals(600000, new CassandraDuration("10m").toMilliseconds());
        assertEquals(600, new CassandraDuration("10m").toSeconds());
    }

    @Test
    public void testInvalidInputs()
    {
        assertThatThrownBy(() -> new CassandraDuration("10")).isInstanceOf(IllegalArgumentException.class)
                                                             .hasMessageContaining("Invalid duration: 10");
        assertThatThrownBy(() -> new CassandraDuration("-10s")).isInstanceOf(IllegalArgumentException.class)
                                                               .hasMessageContaining("Invalid duration: -10s");
        assertThatThrownBy(() -> new CassandraDuration("10xd")).isInstanceOf(IllegalArgumentException.class)
                                                               .hasMessageContaining("Invalid duration: 10xd");
    }

    @Test
    public void testEquals()
    {
        assertEquals(new CassandraDuration("10s"), new CassandraDuration("10s"));
        assertEquals(new CassandraDuration("10s"), new CassandraDuration("10000ms"));
        assertEquals(new CassandraDuration("10000ms"), new CassandraDuration("10s"));
        assertEquals(CassandraDuration.inMinutes(Long.MAX_VALUE), CassandraDuration.inMinutes(Long.MAX_VALUE));
        assertNotEquals(CassandraDuration.inMinutes(Long.MAX_VALUE), CassandraDuration.inMilliseconds(Long.MAX_VALUE));
        assertNotEquals(new CassandraDuration("0m"), new CassandraDuration("10ms"));
    }

    @Test
    public void thereAndBack()
    {
        Gen<TimeUnit> unitGen = SourceDSL.arbitrary().enumValues(TimeUnit.class);
        Gen<Long> valueGen = SourceDSL.longs().between(0, Long.MAX_VALUE);
        qt().forAll(valueGen, unitGen).check((value, unit) -> {
            CassandraDuration there = new CassandraDuration(value, unit);
            CassandraDuration back = new CassandraDuration(there.toString());
            CassandraDuration BACK = new CassandraDuration(there.toString().toUpperCase(Locale.ROOT));
            return there.equals(back) && there.equals(BACK);
        });
    }
}

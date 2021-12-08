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

import org.junit.Test;

import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.*;
import static org.quicktheories.QuickTheory.qt;

public class BitRateTest
{
    @Test
    public void testConversions()
    {
        assertEquals(10, new BitRate("10B/s").toBitsPerSecond());
        assertEquals(10000, new BitRate("10KiB/s").toBitsPerSecond());
        assertEquals(0, new BitRate("10KiB/s").toMegabitsPerSecond());
        assertEquals(10000, new BitRate("10MiB/s").toKilobitsPerSecond());
        assertEquals(10000000, new BitRate("10MiB/s").toBitsPerSecond());
    }

    @Test
    public void testInvalidInputs()
    {
        assertThatThrownBy(() -> new BitRate("10")).isInstanceOf(IllegalArgumentException.class)
                                                   .hasMessageContaining("Invalid bit rate: 10");
        assertThatThrownBy(() -> new BitRate("-10b/s")).isInstanceOf(IllegalArgumentException.class)
                                                       .hasMessageContaining("Invalid bit rate: -10b/s");
        assertThatThrownBy(() -> new BitRate("10xb/s")).isInstanceOf(IllegalArgumentException.class)
                                                       .hasMessageContaining("Invalid bit rate: 10xb/s");

    }

    @Test
    public void testEquals()
    {
        assertEquals(new BitRate("10B/s"), new BitRate("10B/s"));
        assertEquals(new BitRate("10KiB/s"), new BitRate("10000B/s"));
        assertEquals(new BitRate("10000B/s"), new BitRate("10KiB/s"));
        assertEquals(BitRate.inMegabitsPerSecond(Long.MAX_VALUE), BitRate.inMegabitsPerSecond(Long.MAX_VALUE));
        assertNotEquals(BitRate.inMegabitsPerSecond(Long.MAX_VALUE), BitRate.inBitsPerSeconds(Long.MAX_VALUE));
        assertNotEquals(new BitRate("0KiB/s"), new BitRate("10MiB/s"));
    }

    @Test
    public void thereAndBack()
    {
        Gen<BitRate.BitRateUnit> unitGen = SourceDSL.arbitrary().enumValues(BitRate.BitRateUnit.class);
        Gen<Long> valueGen = SourceDSL.longs().between(0, Long.MAX_VALUE);
        qt().forAll(valueGen, unitGen).check((value, unit) -> {
            BitRate there = new BitRate(value, unit);
            BitRate back = new BitRate(there.toString());
            BitRate BACK = new BitRate(there.toString().toUpperCase(Locale.ROOT).replace("I", "i"));
            return there.equals(back) && there.equals(BACK);
        });
    }

    @Test
    public void eq()
    {
        qt().forAll(gen(), gen()).check((a, b) -> a.equals(b) == b.equals(a));
    }

    @Test
    public void eqAndHash()
    {
        qt().forAll(gen(), gen()).check((a, b) -> !a.equals(b) || a.hashCode() == b.hashCode());
    }

    private static Gen<BitRate> gen()
    {
        Gen<BitRate.BitRateUnit> unitGen = SourceDSL.arbitrary().enumValues(BitRate.BitRateUnit.class);
        Gen<Long> valueGen = SourceDSL.longs().between(0, Long.MAX_VALUE);;
        Gen<BitRate> gen = rs -> new BitRate(valueGen.generate(rs), unitGen.generate(rs));
        return gen.describedAs(BitRate::toString);
    }
}

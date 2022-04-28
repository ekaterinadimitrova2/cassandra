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

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.*;
import static org.quicktheories.QuickTheory.qt;

public class DataRateSpecTest
{
    @Test
    public void testConversions()
    {
        assertEquals(10, new DataRateSpec("10B/s").toBytesPerSecond(), 0);
        assertEquals(10240, new DataRateSpec("10KiB/s").toBytesPerSecond(), 0);
        assertEquals(0, new DataRateSpec("10KiB/s").toMebibytesPerSecond(), 0.1);
        assertEquals(10240, new DataRateSpec("10MiB/s").toKibibytesPerSecond(), 0);
        assertEquals(10485760, new DataRateSpec("10MiB/s").toBytesPerSecond(), 0);
        assertEquals(10485760, new DataRateSpec("10MiB/s").toBytesPerSecond(), 0);
        assertEquals(new DataRateSpec("24MiB/s").toString(), DataRateSpec.megabitsPerSecondInMebibytesPerSecond(200L).toString());
    }

    @Test
    public void testOverflowingDuringConversion()
    {
        assertEquals(Integer.MAX_VALUE, new IntDataRate("2147483649B/s").toBytesPerSecondAsInt(), 0);
        assertEquals(Integer.MAX_VALUE, new IntDataRate(2147483649L/1024L + "KiB/s").toBytesPerSecondAsInt(), 0);
        assertEquals(Integer.MAX_VALUE, new IntDataRate(2147483649L/1024L/1024 + "MiB/s").toBytesPerSecondAsInt(), 0);

        assertEquals(Integer.MAX_VALUE, new IntDataRate(2147483647L + "MiB/s").toMegabitsPerSecondAsInt());

        assertEquals(Integer.MAX_VALUE, new IntDataRate(2147483647L + "KiB/s").toKibibytesPerSecondAsInt());
        assertEquals(Integer.MAX_VALUE, new IntDataRate(2147483649L/1024L + "MiB/s").toKibibytesPerSecondAsInt());

        assertEquals(Integer.MAX_VALUE, new IntDataRate(2147483647L + "MiB/s").toMebibytesPerSecondAsInt());
    }

    @Test
    public void testFromSymbol()
    {
        assertEquals(DataRateSpec.DataRateUnit.fromSymbol("B/s"), DataRateSpec.DataRateUnit.BYTES_PER_SECOND);
        assertEquals(DataRateSpec.DataRateUnit.fromSymbol("KiB/s"), DataRateSpec.DataRateUnit.KIBIBYTES_PER_SECOND);
        assertEquals(DataRateSpec.DataRateUnit.fromSymbol("MiB/s"), DataRateSpec.DataRateUnit.MEBIBYTES_PER_SECOND);
        assertThatThrownBy(() -> DataRateSpec.DataRateUnit.fromSymbol("n"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Unsupported data rate unit: n");
    }

    @Test
    public void testInvalidInputs()
    {
        assertThatThrownBy(() -> new DataRateSpec("10")).isInstanceOf(ConfigurationException.class)
                                                        .hasMessageContaining("Invalid data rate: 10");
        assertThatThrownBy(() -> new DataRateSpec("-10b/s")).isInstanceOf(ConfigurationException.class)
                                                            .hasMessageContaining("Invalid data rate: -10b/s");
        assertThatThrownBy(() -> new DataRateSpec("10xb/s")).isInstanceOf(ConfigurationException.class)
                                                            .hasMessageContaining("Invalid data rate: 10xb/s");
        assertThatThrownBy(() -> new DataRateSpec("9223372036854775809B/s")
                                 .toBytesPerSecond()).isInstanceOf(NumberFormatException.class)
                                                     .hasMessageContaining("For input string: \"9223372036854775809\"");
        assertThatThrownBy(() -> new DataRateSpec("9223372036854775809KiB/s")
                                 .toBytesPerSecond()).isInstanceOf(NumberFormatException.class)
                                                     .hasMessageContaining("For input string: \"9223372036854775809\"");
        assertThatThrownBy(() -> new DataRateSpec("9223372036854775809MiB/s")
                                 .toBytesPerSecond()).isInstanceOf(NumberFormatException.class)
                                                     .hasMessageContaining("For input string: \"9223372036854775809\"");
        assertThatThrownBy(() -> new IntDataRate("2147483648MiB/s")
                                 .toBytesPerSecond()).isInstanceOf(NumberFormatException.class)
                                                     .hasMessageContaining("Invalid data rate: value must be between 0 and 2147483647 mebibytes per second");
    }

    @Test
    public void testInvalidForConversion()
    {
        //just test the cast to Int
        assertEquals(Integer.MAX_VALUE, new DataRateSpec("92233720368547758B/s").toBytesPerSecondAsInt());

        assertThatThrownBy(() -> new DataRateSpec(Long.MAX_VALUE + "B/s")).isInstanceOf(NumberFormatException.class)
                                                                         .hasMessageContaining("Invalid data rate: value 9.223372036854776E18 " +
                                                                                               "must be between 0 and 9223372036854775806 bytes per second");
        assertThatThrownBy(() -> new DataRateSpec(Long.MAX_VALUE + "MiB/s")).isInstanceOf(NumberFormatException.class)
                                                                         .hasMessageContaining("Invalid data rate: value 9.223372036854776E18 " +
                                                                                               "must be between 0 and 9223372036854775806 bytes per second");
        assertThatThrownBy(() -> new DataRateSpec(Long.MAX_VALUE-5 + "KiB/s")).isInstanceOf(NumberFormatException.class)
                                                                           .hasMessageContaining("Invalid data rate: value 9.223372036854776E18 " +
                                                                                                 "must be between 0 and 9223372036854775806 bytes per second");
    }

    @Test
    public void testEquals()
    {
        assertEquals(new DataRateSpec("10B/s"), new DataRateSpec("10B/s"));
        assertEquals(new DataRateSpec("10KiB/s"), new DataRateSpec("10240B/s"));
        assertEquals(new DataRateSpec("10240B/s"), new DataRateSpec("10KiB/s"));
        assertEquals(DataRateSpec.inMebibytesPerSecond((Long.MAX_VALUE/1024L/1024)), DataRateSpec.inMebibytesPerSecond(Long.MAX_VALUE/1024L/1024));
        long tmp = Long.MAX_VALUE-1L;
        assertNotEquals(DataRateSpec.inKibibytesPerSecond(Long.MAX_VALUE/1024L), DataRateSpec.inBytesPerSecond(tmp));
        assertNotEquals(new DataRateSpec("0KiB/s"), new DataRateSpec("10MiB/s"));
    }

    @Test
    public void thereAndBack()
    {
        Gen<DataRateSpec.DataRateUnit> unitGen = SourceDSL.arbitrary().enumValues(DataRateSpec.DataRateUnit.class);
        Gen<Long> valueGen = SourceDSL.longs().between(0, Long.MAX_VALUE/1024L/1024/1024); // the biggest value in GiB/s that won't lead to B/s overflow
        qt().forAll(valueGen, unitGen).check((value, unit) -> {
            DataRateSpec there = new DataRateSpec(value, unit);
            DataRateSpec back = new DataRateSpec(there.toString());
            DataRateSpec BACK = new DataRateSpec(there.toString());
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

    private static Gen<DataRateSpec> gen()
    {
        Gen<DataRateSpec.DataRateUnit> unitGen = SourceDSL.arbitrary().enumValues(DataRateSpec.DataRateUnit.class);
        Gen<Long> valueGen = SourceDSL.longs().between(0, Long.MAX_VALUE/1024L/1024/1024);
        Gen<DataRateSpec> gen = rs -> new DataRateSpec(valueGen.generate(rs), unitGen.generate(rs));
        return gen.describedAs(DataRateSpec::toString);
    }
}
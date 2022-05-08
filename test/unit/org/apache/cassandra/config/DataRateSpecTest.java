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
        assertEquals(10, new DataRateSpec.BytesPerSecond("10B/s").toBytesPerSecond(), 0);
        assertEquals(10240, new DataRateSpec.BytesPerSecond("10KiB/s").toBytesPerSecond(), 0);
        assertEquals(0, new DataRateSpec.BytesPerSecond("10KiB/s").toMebibytesPerSecond(), 0.1);
        assertEquals(10240, new DataRateSpec.BytesPerSecond("10MiB/s").toKibibytesPerSecond(), 0);
        assertEquals(10485760, new DataRateSpec.BytesPerSecond("10MiB/s").toBytesPerSecond(), 0);
        assertEquals(10485760, new DataRateSpec.BytesPerSecond("10MiB/s").toBytesPerSecond(), 0);
        assertEquals(new DataRateSpec.BytesPerSecond("24MiB/s").toString(), DataRateSpec.IntMebibytesPerSecond.megabitsPerSecondInMebibytesPerSecond(200L).toString());
    }

    @Test
    public void testOverflowingDuringConversion()
    {
        assertEquals(Integer.MAX_VALUE, new DataRateSpec.IntMebibytesPerSecond("2147483649B/s").toBytesPerSecondAsInt(), 0);
        assertEquals(Integer.MAX_VALUE, new DataRateSpec.IntMebibytesPerSecond(2147483649L / 1024L + "KiB/s").toBytesPerSecondAsInt(), 0);
        assertEquals(Integer.MAX_VALUE, new DataRateSpec.IntMebibytesPerSecond(2147483649L / 1024L / 1024 + "MiB/s").toBytesPerSecondAsInt(), 0);

        assertEquals(Integer.MAX_VALUE, new DataRateSpec.IntMebibytesPerSecond(2147483647L + "MiB/s").toMegabitsPerSecondAsInt());

        assertEquals(Integer.MAX_VALUE, new DataRateSpec.IntMebibytesPerSecond(2147483647L + "KiB/s").toKibibytesPerSecondAsInt());
        assertEquals(Integer.MAX_VALUE, new DataRateSpec.IntMebibytesPerSecond(2147483649L / 1024L + "MiB/s").toKibibytesPerSecondAsInt());

        assertEquals(Integer.MAX_VALUE, new DataRateSpec.IntMebibytesPerSecond(2147483647L + "MiB/s").toMebibytesPerSecondAsInt());

        assertThatThrownBy(() -> new DataRateSpec.IntMebibytesPerSecond("2147483648MiB/s")).isInstanceOf(ConfigurationException.class)
                                                                                           .hasMessageContaining("Invalid data rate: 2147483648MiB/s. " +
                                                                                                                 "It shouldn't be more than 2147483647 in mebibytes_per_second");
        assertThatThrownBy(() -> new DataRateSpec.IntMebibytesPerSecond(2147483648L)).isInstanceOf(ConfigurationException.class)
                                                                                     .hasMessageContaining("Invalid data rate: 2.147483648E9 mebibytes_per_second. " +
                                                                                                                            "It shouldn't be more than 2147483647 in mebibytes_per_second");

        assertThatThrownBy(() -> new DataRateSpec.IntMebibytesPerSecond((Integer.MAX_VALUE * 1024L + 1L) + "KiB/s")).isInstanceOf(ConfigurationException.class)
                                                                                                                    .hasMessageContaining("Invalid data rate: 2199023254529KiB/s. " +
                                                                                                                                          "It shouldn't be more than 2147483647 in mebibytes_per_second");
        assertThatThrownBy(() -> new DataRateSpec.IntMebibytesPerSecond((Integer.MAX_VALUE * 1024L * 1024 + 1L) + "B/s")).isInstanceOf(ConfigurationException.class)
                                                                                                                         .hasMessageContaining("Invalid data rate: 2251799812636673B/s. " +
                                                                                                                                               "It shouldn't be more than 2147483647 in mebibytes_per_second");
        assertThatThrownBy(() -> DataRateSpec.IntMebibytesPerSecond.megabitsPerSecondInMebibytesPerSecond(2147483648L)).isInstanceOf(ConfigurationException.class)
                                                                                                                       .hasMessageContaining("Invalid data rate: 2147483648 megabits per second; " +
                                                                                                                                             "stream_throughput_outbound and " +
                                                                                                                                             "inter_dc_stream_throughput_outbound should " +
                                                                                                                                             "be between 0 and 2147483647 in megabits per second");
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
        assertThatThrownBy(() -> new DataRateSpec.BytesPerSecond("10")).isInstanceOf(ConfigurationException.class)
                                                        .hasMessageContaining("Invalid data rate: 10");
        assertThatThrownBy(() -> new DataRateSpec.BytesPerSecond("-10b/s")).isInstanceOf(ConfigurationException.class)
                                                            .hasMessageContaining("Invalid data rate: -10b/s");
        assertThatThrownBy(() -> new DataRateSpec.BytesPerSecond("10xb/s")).isInstanceOf(ConfigurationException.class)
                                                            .hasMessageContaining("Invalid data rate: 10xb/s");
        assertThatThrownBy(() -> new DataRateSpec.BytesPerSecond("9223372036854775809B/s")
                                 .toBytesPerSecond()).isInstanceOf(NumberFormatException.class)
                                                     .hasMessageContaining("For input string: \"9223372036854775809\"");
        assertThatThrownBy(() -> new DataRateSpec.BytesPerSecond("9223372036854775809KiB/s")
                                 .toBytesPerSecond()).isInstanceOf(NumberFormatException.class)
                                                     .hasMessageContaining("For input string: \"9223372036854775809\"");
        assertThatThrownBy(() -> new DataRateSpec.BytesPerSecond("9223372036854775809MiB/s")
                                 .toBytesPerSecond()).isInstanceOf(NumberFormatException.class)
                                                     .hasMessageContaining("For input string: \"9223372036854775809\"");
        assertThatThrownBy(() -> new DataRateSpec.IntMebibytesPerSecond("2147483648MiB/s")
                                 .toBytesPerSecond()).isInstanceOf(ConfigurationException.class)
                                                     .hasMessageContaining("Invalid data rate: 2147483648MiB/s. It shouldn't be more" +
                                                                           " than 2147483647 in mebibytes_per_second");
    }

    @Test
    public void testInvalidForConversion()
    {
        //just test the cast to Int
        assertEquals(Integer.MAX_VALUE, new DataRateSpec.BytesPerSecond("92233720368547758B/s").toBytesPerSecondAsInt());

        assertThatThrownBy(() -> new DataRateSpec.BytesPerSecond(Long.MAX_VALUE + "B/s")).isInstanceOf(ConfigurationException.class)
                                                                         .hasMessageContaining("Invalid data rate: 9223372036854775807B/s. " +
                                                                                               "It shouldn't be more than 9223372036854775806 in bytes_per_second");
        assertThatThrownBy(() -> new DataRateSpec.BytesPerSecond(Long.MAX_VALUE + "MiB/s")).isInstanceOf(ConfigurationException.class)
                                                                         .hasMessageContaining("Invalid data rate: 9223372036854775807MiB/s. " +
                                                                                               "It shouldn't be more than 9223372036854775806 in bytes_per_second");
        assertThatThrownBy(() -> new DataRateSpec.BytesPerSecond(Long.MAX_VALUE - 5 + "KiB/s")).isInstanceOf(ConfigurationException.class)
                                                                           .hasMessageContaining("Invalid data rate: 9223372036854775802KiB/s. " +
                                                                                                 "It shouldn't be more than 9223372036854775806 in bytes_per_second");
    }

    @Test
    public void testValidUnits()
    {
        // we need toString as internally it is double and they are not 0.0 equal but for the end user the double numbers don't exist
        assertEquals(new DataRateSpec.IntMebibytesPerSecond("24MiB/s").toString(), DataRateSpec.IntMebibytesPerSecond.megabitsPerSecondInMebibytesPerSecond(200).toString());
    }

    @Test
    public void testEquals()
    {
        assertEquals(new DataRateSpec.BytesPerSecond("10B/s"), new DataRateSpec.BytesPerSecond("10B/s"));
        assertEquals(new DataRateSpec.BytesPerSecond("10KiB/s"), new DataRateSpec.BytesPerSecond("10240B/s"));
        assertEquals(new DataRateSpec.BytesPerSecond("10240B/s"), new DataRateSpec.BytesPerSecond("10KiB/s"));
        assertNotEquals(new DataRateSpec.BytesPerSecond("0KiB/s"), new DataRateSpec.BytesPerSecond("10MiB/s"));
    }

    @Test
    public void thereAndBack()
    {
        Gen<DataRateSpec.DataRateUnit> unitGen = SourceDSL.arbitrary().enumValues(DataRateSpec.DataRateUnit.class);
        Gen<Long> valueGen = SourceDSL.longs().between(0, Long.MAX_VALUE/1024L/1024/1024); // the biggest value in GiB/s that won't lead to B/s overflow
        qt().forAll(valueGen, unitGen).check((value, unit) -> {
            DataRateSpec there = new DataRateSpec.BytesPerSecond(value, unit);
            DataRateSpec back = new DataRateSpec.BytesPerSecond(there.toString());
            DataRateSpec BACK = new DataRateSpec.BytesPerSecond(there.toString());
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
        Gen<DataRateSpec> gen = rs -> new DataRateSpec.BytesPerSecond(valueGen.generate(rs), unitGen.generate(rs));
        return gen.describedAs(DataRateSpec::toString);
    }
}
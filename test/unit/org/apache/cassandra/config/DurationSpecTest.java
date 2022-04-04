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

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.*;
import static org.quicktheories.QuickTheory.qt;

public class DurationSpecTest
{
    @Test
    public void testConversions()
    {
        assertEquals(10L, new DurationSpec.LongNanosecondsBound ("10s").toSeconds());
        assertEquals(Integer.MAX_VALUE-1, new DurationSpec.IntSecondsBound(Integer.MAX_VALUE-1 + "s").toSecondsAsInt());
        assertEquals(10000, new DurationSpec.LongNanosecondsBound ("10s").toMilliseconds());
        assertEquals(Integer.MAX_VALUE-1, new DurationSpec.LongMillisecondsBound(Integer.MAX_VALUE-1 + "ms").toMillisecondsAsInt());
        assertEquals(0, new DurationSpec.LongNanosecondsBound ("10s").toMinutes());
        assertEquals(10, new DurationSpec.LongNanosecondsBound ("10m").toMinutes());
        assertEquals(Integer.MAX_VALUE-1, new DurationSpec.IntMinutesBound(Integer.MAX_VALUE-1 + "m").toMinutesAsInt());
        assertEquals(600000, new DurationSpec.LongNanosecondsBound("10m").toMilliseconds());
        assertEquals(600, new DurationSpec.LongNanosecondsBound("10m").toSeconds());
        assertEquals(Integer.MAX_VALUE-1, new DurationSpec.IntSecondsBound(Integer.MAX_VALUE-1 + "s").toSecondsAsInt());
        assertEquals(new DurationSpec.IntMillisecondsBound(0.7, TimeUnit.MILLISECONDS), new DurationSpec.LongNanosecondsBound("1ms"));
        assertEquals(new DurationSpec.IntMillisecondsBound(0.33, TimeUnit.MILLISECONDS), new DurationSpec.LongNanosecondsBound("0ms"));
        assertEquals(new DurationSpec.IntMillisecondsBound(0.333, TimeUnit.MILLISECONDS), new DurationSpec.LongNanosecondsBound("0ms"));
    }

    @Test
    public void testFromSymbol()
    {
        assertEquals(DurationSpec.fromSymbol("ms"), TimeUnit.MILLISECONDS);
        assertEquals(DurationSpec.fromSymbol("d"), TimeUnit.DAYS);
        assertEquals(DurationSpec.fromSymbol("h"), TimeUnit.HOURS);
        assertEquals(DurationSpec.fromSymbol("m"), TimeUnit.MINUTES);
        assertEquals(DurationSpec.fromSymbol("s"), TimeUnit.SECONDS);
        assertEquals(DurationSpec.fromSymbol("us"), TimeUnit.MICROSECONDS);
        assertEquals(DurationSpec.fromSymbol("µs"), TimeUnit.MICROSECONDS);
        assertEquals(DurationSpec.fromSymbol("ns"), TimeUnit.NANOSECONDS);
        assertThatThrownBy(() -> DurationSpec.fromSymbol("n")).isInstanceOf(ConfigurationException.class)
                                                              .hasMessageContaining("Unsupported time unit: n");
    }

    @Test
    public void testGetSymbol()
    {
        assertEquals(DurationSpec.getSymbol(TimeUnit.MILLISECONDS), "ms");
        assertEquals(DurationSpec.getSymbol(TimeUnit.DAYS), "d");
        assertEquals(DurationSpec.getSymbol(TimeUnit.HOURS), "h");
        assertEquals(DurationSpec.getSymbol(TimeUnit.MINUTES), "m");
        assertEquals(DurationSpec.getSymbol(TimeUnit.SECONDS), "s");
        assertEquals(DurationSpec.getSymbol(TimeUnit.MICROSECONDS), "us");
        assertEquals(DurationSpec.getSymbol(TimeUnit.NANOSECONDS), "ns");
    }

    @Test
    public void testInvalidInputs()
    {
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound("10")).isInstanceOf(ConfigurationException.class)
                                                        .hasMessageContaining("Invalid duration: 10");
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound("-10s")).isInstanceOf(ConfigurationException.class)
                                                          .hasMessageContaining("Invalid duration: -10s");
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound("10xd")).isInstanceOf(ConfigurationException.class)
                                                          .hasMessageContaining("Invalid duration: 10xd");
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound("0.333555555ms")).isInstanceOf(ConfigurationException.class)
                                                                   .hasMessageContaining("Invalid duration: 0.333555555ms");
    }

    @Test
    public void testInvalidForConversion()
    {
        //just test the cast to Int
        assertEquals(Integer.MAX_VALUE, new DurationSpec.LongNanosecondsBound("9223372036854775806ns").toNanosecondsAsInt());

        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound(Long.MAX_VALUE + "ns")).isInstanceOf(ConfigurationException.class)
                                                                           .hasMessageContaining("Invalid duration: 9223372036854775807ns. " +
                                                                                                 "It shouldn't be more than 9223372036854775806 in nanoseconds");
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound(Long.MAX_VALUE + "ms")).isInstanceOf(ConfigurationException.class)
                                                                             .hasMessageContaining("Invalid duration: 9223372036854775807ms. " +
                                                                                                   "It shouldn't be more than 9223372036854775806 in nanoseconds");
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound(Long.MAX_VALUE-5 + "µs")).isInstanceOf(ConfigurationException.class)
                                                                               .hasMessageContaining("Invalid duration: 9223372036854775802µs. " +
                                                                                                     "It shouldn't be more than 9223372036854775806 in nanoseconds");
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound(Long.MAX_VALUE-5 + "us")).isInstanceOf(ConfigurationException.class)
                                                                               .hasMessageContaining("Invalid duration: 9223372036854775802us. " +
                                                                                                     "It shouldn't be more than 9223372036854775806 in nanoseconds");
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound(Long.MAX_VALUE-5 + "s")).isInstanceOf(ConfigurationException.class)
                                                                           .hasMessageContaining("Invalid duration: 9223372036854775802s. " +
                                                                                                 "It shouldn't be more than 9223372036854775806 in nanoseconds");
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound(Long.MAX_VALUE-5 + "h")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: 9223372036854775802h. " +
                                                                                                "It shouldn't be more than 9223372036854775806 in nanoseconds");
        assertThatThrownBy(() -> new DurationSpec.LongNanosecondsBound(Long.MAX_VALUE-5 + "d")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: 9223372036854775802d. " +
                                                                                                "It shouldn't be more than 9223372036854775806 in nanoseconds");
    }

    @Test
    public void testOverflowingConversion()
    {
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound("2147483648ms")).isInstanceOf(ConfigurationException.class)
                                                                                       .hasMessageContaining("Invalid duration: 2147483648ms." +
                                                                                                             " It shouldn't be more than 2147483646 in milliseconds");
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound(2147483648L)).isInstanceOf(ConfigurationException.class)
                                                                                    .hasMessageContaining("Invalid duration: 2147483648 milliseconds. " +
                                                                                                          "It shouldn't be more than 2147483646 in milliseconds");
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound("2147483648s")).isInstanceOf(ConfigurationException.class)
                                                                                      .hasMessageContaining("Invalid duration: 2147483648s. " +
                                                                                                            "It shouldn't be more than 2147483646 in milliseconds");
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound("35791395m")).isInstanceOf(ConfigurationException.class)
                                                                                    .hasMessageContaining("Invalid duration: 35791395m. " +
                                                                                                          "It shouldn't be more than 2147483646 in milliseconds");
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound("597h")).isInstanceOf(ConfigurationException.class)
                                                                               .hasMessageContaining("Invalid duration: 597h. " +
                                                                                                     "It shouldn't be more than 2147483646 in milliseconds");
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound("24856d")).isInstanceOf(ConfigurationException.class)
                                                                                 .hasMessageContaining("Invalid duration: 24856d. " +
                                                                                                       "It shouldn't be more than 2147483646 in milliseconds");

        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound("2147483648s")).isInstanceOf(ConfigurationException.class)
                                                                                 .hasMessageContaining("Invalid duration: 2147483648s. " +
                                                                                                       "It shouldn't be more than 2147483646 in seconds");
        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound(2147483648L)).isInstanceOf(ConfigurationException.class)
                                                                               .hasMessageContaining("Invalid duration: 2147483648 seconds. " +
                                                                                                     "It shouldn't be more than 2147483646 in seconds");
        assertThatThrownBy(() -> DurationSpec.IntSecondsBound.inSecondsString("2147483648")).isInstanceOf(NumberFormatException.class)
                                                                                            .hasMessageContaining("For input string: \"2147483648\"");
        assertThatThrownBy(() -> DurationSpec.IntSecondsBound.inSecondsString("2147483648s")).isInstanceOf(ConfigurationException.class)
                                                                                             .hasMessageContaining("Invalid duration: 2147483648s. " +
                                                                                                                   "It shouldn't be more than 2147483646 in seconds");
        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound("35791395m")).isInstanceOf(ConfigurationException.class)
                                                                               .hasMessageContaining("Invalid duration: 35791395m. " +
                                                                                                     "It shouldn't be more than 2147483646 in seconds");
        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound("596524h")).isInstanceOf(ConfigurationException.class)
                                                                             .hasMessageContaining("Invalid duration: 596524h. " +
                                                                                                   "It shouldn't be more than 2147483646 in seconds");
        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound("24856d")).isInstanceOf(ConfigurationException.class)
                                                                            .hasMessageContaining("Invalid duration: 24856d. " +
                                                                                                  "It shouldn't be more than 2147483646 in seconds");

        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound("2147483648s")).isInstanceOf(ConfigurationException.class)
                                                                                 .hasMessageContaining("Invalid duration: 2147483648s " +
                                                                                                       "Accepted units:[MINUTES, HOURS, DAYS]");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound("2147483648m")).isInstanceOf(ConfigurationException.class)
                                                                                 .hasMessageContaining("Invalid duration: 2147483648m. " +
                                                                                                       "It shouldn't be more than 2147483646 in minutes");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound("35791395h")).isInstanceOf(ConfigurationException.class)
                                                                               .hasMessageContaining("Invalid duration: 35791395h. " +
                                                                                                     "It shouldn't be more than 2147483646 in minutes");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound("1491309d")).isInstanceOf(ConfigurationException.class)
                                                                              .hasMessageContaining("Invalid duration: 1491309d. " +
                                                                                                    "It shouldn't be more than 2147483646 in minutes");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound(2147483648L)).isInstanceOf(ConfigurationException.class)
                                                                               .hasMessageContaining("Invalid duration: 2147483648 minutes. " +
                                                                                                     "It shouldn't be more than 2147483646 in minutes");
    }

    @Test
    public void testEquals()
    {
        assertEquals(new DurationSpec.LongNanosecondsBound ("10s"), new DurationSpec.LongNanosecondsBound ("10s"));
        assertEquals(new DurationSpec.LongNanosecondsBound ("10s"), new DurationSpec.LongNanosecondsBound ("10000ms"));
        assertEquals(new DurationSpec.LongNanosecondsBound ("10000ms"), new DurationSpec.LongNanosecondsBound ("10s"));
        assertEquals(new DurationSpec.LongNanosecondsBound ("4h"), new DurationSpec.LongNanosecondsBound ("14400s"));
        assertEquals(DurationSpec.LongNanosecondsBound .IntSecondsBound.inSecondsString("14400"), new DurationSpec.LongNanosecondsBound ("14400s"));
        assertEquals(DurationSpec.LongNanosecondsBound .IntSecondsBound.inSecondsString("4h"), new DurationSpec.LongNanosecondsBound ("14400s"));
        assertEquals(DurationSpec.LongNanosecondsBound .IntSecondsBound.inSecondsString("14400s"), new DurationSpec.LongNanosecondsBound ("14400s"));
        assertNotEquals(new DurationSpec.LongNanosecondsBound ("0m"), new DurationSpec.LongNanosecondsBound ("10ms"));
        assertEquals(Long.MAX_VALUE-1, new DurationSpec.LongNanosecondsBound ("9223372036854775806ns").toNanoseconds());
        assertEquals(Integer.MAX_VALUE, new DurationSpec.LongNanosecondsBound ("9223372036854775806ns").toNanosecondsAsInt());
    }

    @Test
    public void thereAndBack()
    {
        Gen<TimeUnit> unitGen = SourceDSL.arbitrary().enumValues(TimeUnit.class);
        Gen<Long> valueGen = SourceDSL.longs().between(0, Long.MAX_VALUE/24/60/60/1000L/1000L/1000L);
        qt().forAll(valueGen, unitGen).check((value, unit) -> {
            DurationSpec.LongNanosecondsBound  there = new DurationSpec.LongNanosecondsBound (value, unit);
            DurationSpec.LongNanosecondsBound  back = new DurationSpec.LongNanosecondsBound (there.toString());
            return there.equals(back);
        });
    }

    @Test
    public void testValidUnits()
    {
        assertEquals(10L, new DurationSpec.IntMillisecondsBound("10ms").toMilliseconds());
        assertEquals(10L, new DurationSpec.IntSecondsBound("10s").toSeconds());
        assertEquals(new DurationSpec.IntSecondsBound("10s"), DurationSpec.IntSecondsBound.inSecondsString("10"));
        assertEquals(new DurationSpec.IntSecondsBound("10s"), DurationSpec.IntSecondsBound.inSecondsString("10s"));

        assertEquals(10L, new DurationSpec.LongMillisecondsBound("10ms").toMilliseconds());
        assertEquals(10L, new DurationSpec.LongSecondsBound("10s").toSeconds());
    }

    @Test
    public void testInvalidUnits()
    {
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound("10ns")).isInstanceOf(ConfigurationException.class)
                                                                               .hasMessageContaining("Invalid duration: 10ns");
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound("10us")).isInstanceOf(ConfigurationException.class)
                                                                               .hasMessageContaining("Invalid duration: 10us");
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound("10µs")).isInstanceOf(ConfigurationException.class)
                                                                               .hasMessageContaining("Invalid duration: 10µs");
        assertThatThrownBy(() -> new DurationSpec.IntMillisecondsBound("-10s")).isInstanceOf(ConfigurationException.class)
                                                                               .hasMessageContaining("Invalid duration: -10s");

        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound("10ms")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: 10ms");
        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound("10ns")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: 10ns");
        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound("10us")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: 10us");
        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound("10µs")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: 10µs");
        assertThatThrownBy(() -> new DurationSpec.IntSecondsBound("-10s")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: -10s");

        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound("10s")).isInstanceOf(ConfigurationException.class)
                                                                         .hasMessageContaining("Invalid duration: 10s");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound("10ms")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: 10ms");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound("10ns")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: 10ns");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound("10us")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: 10us");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound("10µs")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: 10µs");
        assertThatThrownBy(() -> new DurationSpec.IntMinutesBound("-10s")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: -10s");

        assertThatThrownBy(() -> new DurationSpec.LongMillisecondsBound("10ns")).isInstanceOf(ConfigurationException.class)
                                                                                .hasMessageContaining("Invalid duration: 10ns");
        assertThatThrownBy(() -> new DurationSpec.LongMillisecondsBound("10us")).isInstanceOf(ConfigurationException.class)
                                                                                .hasMessageContaining("Invalid duration: 10us");
        assertThatThrownBy(() -> new DurationSpec.LongMillisecondsBound("10µs")).isInstanceOf(ConfigurationException.class)
                                                                                .hasMessageContaining("Invalid duration: 10µs");
        assertThatThrownBy(() -> new DurationSpec.LongMillisecondsBound("-10s")).isInstanceOf(ConfigurationException.class)
                                                                                .hasMessageContaining("Invalid duration: -10s");

        assertThatThrownBy(() -> new DurationSpec.LongSecondsBound("10ms")).isInstanceOf(ConfigurationException.class)
                                                                           .hasMessageContaining("Invalid duration: 10ms");
        assertThatThrownBy(() -> new DurationSpec.LongSecondsBound("10ns")).isInstanceOf(ConfigurationException.class)
                                                                           .hasMessageContaining("Invalid duration: 10ns");
        assertThatThrownBy(() -> new DurationSpec.LongSecondsBound("10us")).isInstanceOf(ConfigurationException.class)
                                                                           .hasMessageContaining("Invalid duration: 10us");
        assertThatThrownBy(() -> new DurationSpec.LongSecondsBound("10µs")).isInstanceOf(ConfigurationException.class)
                                                                           .hasMessageContaining("Invalid duration: 10µs");
        assertThatThrownBy(() -> new DurationSpec.LongSecondsBound("-10s")).isInstanceOf(ConfigurationException.class)
                                                                           .hasMessageContaining("Invalid duration: -10s");
    }
}

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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class SmallestDurationTest
{
    @Test
    public void testInvalidUnits()
    {
        assertThatThrownBy(() -> new SmallestDuration.IntMilliseconds("10ns")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: 10ns");
        assertThatThrownBy(() -> new SmallestDuration.IntMilliseconds("10us")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: 10us");
        assertThatThrownBy(() -> new SmallestDuration.IntMilliseconds("10µs")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: 10µs");
        assertThatThrownBy(() -> new SmallestDuration.IntMilliseconds("-10s")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: -10s");

        assertThatThrownBy(() -> new SmallestDuration.IntSeconds("10ms")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10ms");
        assertThatThrownBy(() -> new SmallestDuration.IntSeconds("10ns")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10ns");
        assertThatThrownBy(() -> new SmallestDuration.IntSeconds("10us")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10us");
        assertThatThrownBy(() -> new SmallestDuration.IntSeconds("10µs")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10µs");
        assertThatThrownBy(() -> new SmallestDuration.IntSeconds("-10s")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: -10s");

        assertThatThrownBy(() -> new SmallestDuration.IntMinutes("10s")).isInstanceOf(ConfigurationException.class)
                                                                    .hasMessageContaining("Invalid duration: 10s");
        assertThatThrownBy(() -> new SmallestDuration.IntMinutes("10ms")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10ms");
        assertThatThrownBy(() -> new SmallestDuration.IntMinutes("10ns")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10ns");
        assertThatThrownBy(() -> new SmallestDuration.IntMinutes("10us")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10us");
        assertThatThrownBy(() -> new SmallestDuration.IntMinutes("10µs")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10µs");
        assertThatThrownBy(() -> new SmallestDuration.IntMinutes("-10s")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: -10s");

        assertThatThrownBy(() -> new SmallestDuration.Milliseconds("10ns")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: 10ns");
        assertThatThrownBy(() -> new SmallestDuration.Milliseconds("10us")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: 10us");
        assertThatThrownBy(() -> new SmallestDuration.Milliseconds("10µs")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: 10µs");
        assertThatThrownBy(() -> new SmallestDuration.Milliseconds("-10s")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: -10s");

        assertThatThrownBy(() -> new SmallestDuration.Minutes("10s")).isInstanceOf(ConfigurationException.class)
                                                                    .hasMessageContaining("Invalid duration: 10s");
        assertThatThrownBy(() -> new SmallestDuration.Minutes("10ms")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10ms");
        assertThatThrownBy(() -> new SmallestDuration.Minutes("10ns")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10ns");
        assertThatThrownBy(() -> new SmallestDuration.Minutes("10us")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10us");
        assertThatThrownBy(() -> new SmallestDuration.Minutes("10µs")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10µs");
        assertThatThrownBy(() -> new SmallestDuration.Minutes("-10s")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: -10s");

        assertThatThrownBy(() -> new SmallestDuration.Seconds("10ms")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10ms");
        assertThatThrownBy(() -> new SmallestDuration.Seconds("10ns")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10ns");
        assertThatThrownBy(() -> new SmallestDuration.Seconds("10us")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10us");
        assertThatThrownBy(() -> new SmallestDuration.Seconds("10µs")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10µs");
        assertThatThrownBy(() -> new SmallestDuration.Seconds("-10s")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: -10s");
    }

    @Test
    public void testOverflowingConversion()
    {
        assertThatThrownBy(() -> new SmallestDuration.IntMilliseconds("2147483648ms")).isInstanceOf(ConfigurationException.class)
                                                                                     .hasMessageContaining("Invalid duration: 2147483648ms." +
                                                                                                           " It shouldn't be more than 2147483647 in milliseconds");
        assertThatThrownBy(() -> new SmallestDuration.IntMilliseconds(2147483648L)).isInstanceOf(ConfigurationException.class)
                                                                                             .hasMessageContaining("Invalid duration: 2147483648 milliseconds. " +
                                                                                                                   "It shouldn't be more than 2147483647 in milliseconds");
        assertThatThrownBy(() -> new SmallestDuration.IntMilliseconds("2147483648s")).isInstanceOf(ConfigurationException.class)
                                                                                    .hasMessageContaining("Invalid duration: 2147483648s. " +
                                                                                                          "It shouldn't be more than 2147483647 in milliseconds");
        assertThatThrownBy(() -> new SmallestDuration.IntMilliseconds("35791395m")).isInstanceOf(ConfigurationException.class)
                                                                                  .hasMessageContaining("Invalid duration: 35791395m. " +
                                                                                                        "It shouldn't be more than 2147483647 in milliseconds");
        assertThatThrownBy(() -> new SmallestDuration.IntMilliseconds("597h")).isInstanceOf(ConfigurationException.class)
                                                                             .hasMessageContaining("Invalid duration: 597h. " +
                                                                                                   "It shouldn't be more than 2147483647 in milliseconds");
        assertThatThrownBy(() -> new SmallestDuration.IntMilliseconds("24856d")).isInstanceOf(ConfigurationException.class)
                                                                               .hasMessageContaining("Invalid duration: 24856d. " +
                                                                                                     "It shouldn't be more than 2147483647 in milliseconds");

        assertThatThrownBy(() -> new SmallestDuration.IntSeconds("2147483648s")).isInstanceOf(ConfigurationException.class)
                                                                               .hasMessageContaining("Invalid duration: 2147483648s. " +
                                                                                                     "It shouldn't be more than 2147483647 in seconds");
        assertThatThrownBy(() -> new SmallestDuration.IntSeconds(2147483648L)).isInstanceOf(ConfigurationException.class)
                                                                               .hasMessageContaining("Invalid duration: 2147483648 seconds. " +
                                                                                                     "It shouldn't be more than 2147483647 in seconds");
        assertThatThrownBy(() -> SmallestDuration.IntSeconds.inSecondsString("2147483648")).isInstanceOf(NumberFormatException.class)
                                                                                   .hasMessageContaining("For input string: \"2147483648\"");
        assertThatThrownBy(() -> SmallestDuration.IntSeconds.inSecondsString("2147483648s")).isInstanceOf(ConfigurationException.class)
                                                                                          .hasMessageContaining("Invalid duration: values must be " +
                                                                                                                "less than 2147483647 seconds, " +
                                                                                                                "but it was 2147483648 seconds");
        assertThatThrownBy(() -> new SmallestDuration.IntSeconds("35791395m")).isInstanceOf(ConfigurationException.class)
                                                                             .hasMessageContaining("Invalid duration: values must be " +
                                                                                                   "less than 2147483647 seconds, " +
                                                                                                   "but it was 2147483700 seconds");
        assertThatThrownBy(() -> new SmallestDuration.IntSeconds("596524h")).isInstanceOf(ConfigurationException.class)
                                                                           .hasMessageContaining("Invalid duration: values must be " +
                                                                                                 "less than 2147483647 seconds, " +
                                                                                                 "but it was 2147486400 seconds");
        assertThatThrownBy(() -> new SmallestDuration.IntSeconds("24856d")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: values must be " +
                                                                                                "less than 2147483647 seconds, " +
                                                                                                "but it was 2147558400 seconds");

        assertThatThrownBy(() -> new SmallestDuration.IntMinutes("2147483648s")).isInstanceOf(ConfigurationException.class)
                                                                               .hasMessageContaining("Invalid duration: 2147483648s " +
                                                                                                     "Accepted units:[MINUTES, HOURS, DAYS]");
        assertThatThrownBy(() -> new SmallestDuration.IntMinutes("2147483648m")).isInstanceOf(ConfigurationException.class)
                                                                               .hasMessageContaining("Invalid duration: values must be " +
                                                                                                     "less than 2147483647 minutes, " +
                                                                                                     "but it was 2147483648 minutes");
        assertThatThrownBy(() -> new SmallestDuration.IntMinutes("35791395h")).isInstanceOf(ConfigurationException.class)
                                                                             .hasMessageContaining("Invalid duration: values must be " +
                                                                                                   "less than 2147483647 minutes, " +
                                                                                                   "but it was 2147483700 minutes");
        assertThatThrownBy(() -> new SmallestDuration.IntMinutes("1491309d")).isInstanceOf(ConfigurationException.class)
                                                                            .hasMessageContaining("Invalid duration: values must be " +
                                                                                                  "less than 2147483647 minutes, " +
                                                                                                  "but it was 2147484960 minutes");
        assertThatThrownBy(() -> new SmallestDuration.IntMinutes(2147483648L)).isInstanceOf(ConfigurationException.class)
                                                                                   .hasMessageContaining("Invalid duration: values must be " +
                                                                                                         "less than 2147483647 minutes, " +
                                                                                                         "but it was 2147483648 minutes");
    }

    @Test
    public void testValidUnits()
    {
        assertEquals(10L, new SmallestDuration.IntMilliseconds("10ms").toMilliseconds());
        assertEquals(10L, new SmallestDuration.IntSeconds("10s").toSeconds());
        assertEquals(new SmallestDuration.IntSeconds("10s"), SmallestDuration.IntSeconds.inSecondsString("10"));
        assertEquals(new SmallestDuration.IntSeconds("10s"), SmallestDuration.IntSeconds.inSecondsString("10s"));
        assertEquals(10L, new SmallestDuration.Minutes("10m").toMinutes());

        assertEquals(10L, new SmallestDuration.Milliseconds("10ms").toMilliseconds());
        assertEquals(10L, new SmallestDuration.Minutes("10m").toMinutes());
        assertEquals(10L, new SmallestDuration.Seconds("10s").toSeconds());
    }
}

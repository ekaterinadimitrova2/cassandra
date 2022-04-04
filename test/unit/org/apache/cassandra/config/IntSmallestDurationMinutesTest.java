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

public class IntSmallestDurationMinutesTest
{
    @Test
    public void testInvalidUnits()
    {
        assertThatThrownBy(() -> new SmallestDurationMinutes("10s")).isInstanceOf(ConfigurationException.class)
                                                                    .hasMessageContaining("Invalid duration: 10s");
        assertThatThrownBy(() -> new SmallestDurationMinutes("10ms")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10ms");
        assertThatThrownBy(() -> new SmallestDurationMinutes("10ns")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10ns");
        assertThatThrownBy(() -> new SmallestDurationMinutes("10us")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10us");
        assertThatThrownBy(() -> new SmallestDurationMinutes("10µs")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10µs");
        assertThatThrownBy(() -> new SmallestDurationMinutes("-10s")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: -10s");
    }

    @Test
    public void testOverflowingConversion()
    {
        assertThatThrownBy(() -> new IntSmallestDurationMinutes("2147483648s")).isInstanceOf(ConfigurationException.class)
                                                                               .hasMessageContaining("Invalid duration: 2147483648s " +
                                                                                                     "Accepted units:[MINUTES, HOURS, DAYS]");
        assertThatThrownBy(() -> new IntSmallestDurationMinutes("2147483648m")).isInstanceOf(ConfigurationException.class)
                                                                                .hasMessageContaining("Invalid duration: values must be " +
                                                                                                      "less than 2147483647 minutes, " +
                                                                                                      "but it was 2147483648 minutes");
        assertThatThrownBy(() -> new IntSmallestDurationMinutes("35791395h")).isInstanceOf(ConfigurationException.class)
                                                                             .hasMessageContaining("Invalid duration: values must be " +
                                                                                                   "less than 2147483647 minutes, " +
                                                                                                   "but it was 2147483700 minutes");
        assertThatThrownBy(() -> new IntSmallestDurationMinutes("1491309d")).isInstanceOf(ConfigurationException.class)
                                                                            .hasMessageContaining("Invalid duration: values must be " +
                                                                                                  "less than 2147483647 minutes, " +
                                                                                                  "but it was 2147484960 minutes");
        assertThatThrownBy(() -> IntSmallestDurationMinutes.inMinutes(2147483648L)).isInstanceOf(ConfigurationException.class)
                                                                                   .hasMessageContaining("Invalid duration: values must be " +
                                                                                                         "less than 2147483647 minutes, " +
                                                                                                         "but it was 2147483648 minutes");
    }

    @Test
    public void testValidUnits()
    {
        assertEquals(10L, new SmallestDurationMinutes("10m").toMinutes());
    }
}

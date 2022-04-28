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

public class IntSmallestDurationSecondsTest
{
    @Test
    public void testInvalidUnits()
    {
        assertThatThrownBy(() -> new IntSmallestDurationSeconds("10ms")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10ms");
        assertThatThrownBy(() -> new IntSmallestDurationSeconds("10ns")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10ns");
        assertThatThrownBy(() -> new IntSmallestDurationSeconds("10us")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10us");
        assertThatThrownBy(() -> new IntSmallestDurationSeconds("10µs")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: 10µs");
        assertThatThrownBy(() -> new IntSmallestDurationSeconds("-10s")).isInstanceOf(ConfigurationException.class)
                                                                     .hasMessageContaining("Invalid duration: -10s");
    }

    @Test
    public void testOverflowingConversion()
    {
        assertThatThrownBy(() -> new IntSmallestDurationSeconds("2147483648s")).isInstanceOf(ConfigurationException.class)
                                                                               .hasMessageContaining("Invalid duration: values must be " +
                                                                                                     "less than 2147483647 seconds, " +
                                                                                                     "but it was 2147483648 seconds");
        assertThatThrownBy(() -> IntSmallestDurationSeconds.inSeconds(2147483648L)).isInstanceOf(ConfigurationException.class)
                                                                               .hasMessageContaining("Invalid duration: values must be " +
                                                                                                     "less than 2147483647 seconds, " +
                                                                                                     "but it was 2147483648 seconds");
        assertThatThrownBy(() -> IntSmallestDurationSeconds.inSecondsString("2147483648")).isInstanceOf(NumberFormatException.class)
                                                                                   .hasMessageContaining("For input string: \"2147483648\"");
        assertThatThrownBy(() -> IntSmallestDurationSeconds.inSecondsString("2147483648s")).isInstanceOf(ConfigurationException.class)
                                                                                          .hasMessageContaining("Invalid duration: values must be " +
                                                                                                                "less than 2147483647 seconds, " +
                                                                                                                "but it was 2147483648 seconds");
        assertThatThrownBy(() -> new IntSmallestDurationSeconds("35791395m")).isInstanceOf(ConfigurationException.class)
                                                                             .hasMessageContaining("Invalid duration: values must be " +
                                                                                                   "less than 2147483647 seconds, " +
                                                                                                   "but it was 2147483700 seconds");
        assertThatThrownBy(() -> new IntSmallestDurationSeconds("596524h")).isInstanceOf(ConfigurationException.class)
                                                                           .hasMessageContaining("Invalid duration: values must be " +
                                                                                                 "less than 2147483647 seconds, " +
                                                                                                 "but it was 2147486400 seconds");
        assertThatThrownBy(() -> new IntSmallestDurationSeconds("24856d")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: values must be " +
                                                                                                "less than 2147483647 seconds, " +
                                                                                                "but it was 2147558400 seconds");
    }

    @Test
    public void testValidUnits()
    {
        assertEquals(10L, new IntSmallestDurationSeconds("10s").toSeconds());
        assertEquals(new IntSmallestDurationSeconds("10s"), IntSmallestDurationSeconds.inSecondsString("10"));
        assertEquals(new IntSmallestDurationSeconds("10s"), IntSmallestDurationSeconds.inSecondsString("10s"));
    }
}

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

public class IntSmallestDurationNanosecondsTest
{
    @Test
    public void testInvalidUnits()
    {
        assertThatThrownBy(() -> new IntSmallestDurationNanoseconds("-10s")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: -10s");
    }

    @Test
    public void testOverflowingConversion()
    {
        assertThatThrownBy(() -> new IntSmallestDurationNanoseconds("2147483648ns")).isInstanceOf(ConfigurationException.class)
                                                                                    .hasMessageContaining("Invalid duration: values must be " +
                                                                                                          "less than 2147483647 nanoseconds, " +
                                                                                                          "but it was 2147483648 nanoseconds");
        assertThatThrownBy(() -> new IntSmallestDurationNanoseconds("2147483648ms")).isInstanceOf(ConfigurationException.class)
                                                                               .hasMessageContaining("Invalid duration: values must be " +
                                                                                                     "less than 2147483647 nanoseconds, " +
                                                                                                     "but it was 2147483648000000 nanoseconds");
        assertThatThrownBy(() -> new IntSmallestDurationNanoseconds("2147483648s")).isInstanceOf(ConfigurationException.class)
                                                                                .hasMessageContaining("Invalid duration: values must be " +
                                                                                                      "less than 2147483647 nanoseconds, " +
                                                                                                      "but it was 2147483648000000000 nanoseconds");
        assertThatThrownBy(() -> new IntSmallestDurationNanoseconds("35791395m")).isInstanceOf(ConfigurationException.class)
                                                                             .hasMessageContaining("Invalid duration: values must be " +
                                                                                                   "less than 2147483647 nanoseconds, " +
                                                                                                   "but it was 2147483700000000000 nanoseconds");
        assertThatThrownBy(() -> new IntSmallestDurationNanoseconds("597h")).isInstanceOf(ConfigurationException.class)
                                                                           .hasMessageContaining("Invalid duration: values must be " +
                                                                                                 "less than 2147483647 nanoseconds, " +
                                                                                                 "but it was 2149200000000000 nanoseconds");
        assertThatThrownBy(() -> new IntSmallestDurationNanoseconds("24856d")).isInstanceOf(ConfigurationException.class)
                                                                          .hasMessageContaining("Invalid duration: values must be " +
                                                                                                "less than 2147483647 nanoseconds, " +
                                                                                                "but it was 2147558400000000000 nanoseconds");
    }

    @Test
    public void testValidUnits()
    {
        assertEquals(10000000L, new IntSmallestDurationNanoseconds("10ms").toNanosecondsAsInt());
    }
}

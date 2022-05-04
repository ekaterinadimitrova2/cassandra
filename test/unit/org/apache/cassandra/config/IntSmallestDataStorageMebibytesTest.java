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
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit;

public class IntSmallestDataStorageMebibytesTest
{
    @Test
    public void testInvalidUnits()
    {
        assertThatThrownBy(() -> new SmallestDataStorage.IntMebibytes("10B")).isInstanceOf(ConfigurationException.class)
                                                                             .hasMessageContaining("Invalid data storage: 10B");
    }

    @Test
    public void testOverflowingConversion()
    {
        assertThatThrownBy(() -> new SmallestDataStorage.IntMebibytes("2147483648B")).isInstanceOf(ConfigurationException.class)
                                                                                     .hasMessageContaining("Invalid data storage: 2147483648B " +
                                                                                                           "Accepted units:[MEBIBYTES, GIBIBYTES]");
        assertThatThrownBy(() -> new SmallestDataStorage.IntMebibytes("2147483648KiB")).isInstanceOf(ConfigurationException.class)
                                                                                       .hasMessageContaining("Invalid data storage: 2147483648KiB " +
                                                                                                             "Accepted units:[MEBIBYTES, GIBIBYTES]");
        assertThatThrownBy(() -> new SmallestDataStorage.IntMebibytes("2147483648MiB")).isInstanceOf(ConfigurationException.class)
                                                                                       .hasMessageContaining("Invalid data storage: values must be less than 2147483647" +
                                                                                                             " mebibytes, but it was 2147483648 mebibytes");
        assertThatThrownBy(() -> new SmallestDataStorage.IntMebibytes("2097152GiB")).isInstanceOf(ConfigurationException.class)
                                                                                    .hasMessageContaining("Invalid data storage: values must be less than 2147483647 " +
                                                                                                          "mebibytes, but it was 2147483648 mebibytes");
        assertThatThrownBy(() -> new SmallestDataStorage.IntMebibytes(2147483648L, DataStorageUnit.MEBIBYTES)).isInstanceOf(ConfigurationException.class)
                                                                                                                              .hasMessageContaining("Invalid data storage: values must be " +
                                                                                                                 "less than 2147483647 mebibytes, " +
                                                                                                                 "but it was 2147483648 mebibytes");
        assertThatThrownBy(() -> new SmallestDataStorage.IntMebibytes(2147483648L * 1024L * 1024, DataStorageUnit.BYTES)).isInstanceOf(ConfigurationException.class)
                                                                                                                                         .hasMessageContaining("Invalid data storage: values must be " +
                                                                                                                            "less than 2147483647 mebibytes, " +
                                                                                                                            "but it was 2147483648 mebibytes");
    }

    @Test
    public void testValidUnits()
    {
        assertEquals(10L, new SmallestDataStorage.IntMebibytes("10MiB").toMebibytes());
    }
}

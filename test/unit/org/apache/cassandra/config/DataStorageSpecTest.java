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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.*;

public class DataStorageSpecTest
{
    @Test
    public void testConversions()
    {
        assertEquals(10, new DataStorageSpec("10B").toBytes());
        assertEquals(10240, new DataStorageSpec("10KB").toBytes());
        assertEquals(0, new DataStorageSpec("10KB").toMegabytes());
        assertEquals(10240, new DataStorageSpec("10MB").toKilobytes());
        assertEquals(10485760, new DataStorageSpec("10MB").toBytes());
    }

    @Test
    public void testInvalidInputs()
    {
        assertThatThrownBy(() -> new DataStorageSpec("10")).isInstanceOf(IllegalArgumentException.class)
                                                           .hasMessageContaining("Invalid data storage: 10");
        assertThatThrownBy(() -> new DataStorageSpec("-10bps")).isInstanceOf(IllegalArgumentException.class)
                                                               .hasMessageContaining("Invalid data storage: -10bps");
        assertThatThrownBy(() -> new DataStorageSpec("-10b")).isInstanceOf(IllegalArgumentException.class)
                                                             .hasMessageContaining("Invalid data storage: -10b");
        assertThatThrownBy(() -> new DataStorageSpec("10HG")).isInstanceOf(IllegalArgumentException.class)
                                                             .hasMessageContaining("Invalid data storage: 10HG");
    }

    @Test
    public void testEquals()
    {
        assertEquals(new DataStorageSpec("10B"), new DataStorageSpec("10B"));
        assertEquals(new DataStorageSpec("10KB"), new DataStorageSpec("10240B"));
        assertEquals(new DataStorageSpec("10240B"), new DataStorageSpec("10KB"));
        assertEquals(DataStorageSpec.inMegabytes(Long.MAX_VALUE), DataStorageSpec.inMegabytes(Long.MAX_VALUE));
        assertNotEquals(DataStorageSpec.inMegabytes(Long.MAX_VALUE), DataStorageSpec.inBytes(Long.MAX_VALUE));
        assertNotEquals(new DataStorageSpec("0MB"), new DataStorageSpec("10KB"));
    }

}

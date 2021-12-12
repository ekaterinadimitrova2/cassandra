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

public class DataStorageSpecTest
{
    @Test
    public void testConversions()
    {
        assertEquals(10, new DataStorageSpec("10B").toBytes());
        assertEquals(10240, new DataStorageSpec("10KiB").toBytes());
        assertEquals(0, new DataStorageSpec("10KiB").toMebibytes());
        assertEquals(10240, new DataStorageSpec("10MiB").toKibibytes());
        assertEquals(10485760, new DataStorageSpec("10MiB").toBytes());
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
        assertEquals(new DataStorageSpec("10KiB"), new DataStorageSpec("10240B"));
        assertEquals(new DataStorageSpec("10240B"), new DataStorageSpec("10KiB"));
        assertEquals(DataStorageSpec.inMebibytes(Long.MAX_VALUE), DataStorageSpec.inMebibytes(Long.MAX_VALUE));
        assertNotEquals(DataStorageSpec.inMebibytes(Long.MAX_VALUE), DataStorageSpec.inBytes(Long.MAX_VALUE));
        assertNotEquals(new DataStorageSpec("0MiB"), new DataStorageSpec("10KiB"));
    }

    @Test
    public void thereAndBack()
    {
        qt().forAll(gen()).check(there -> {
            DataStorageSpec back = new DataStorageSpec(there.toString());
            DataStorageSpec BACK = new DataStorageSpec(there.toString().toUpperCase(Locale.ROOT).replace("I", "i"));
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
        qt().forAll(gen(), gen()).check((a, b) -> a.equals(b) ? a.hashCode() == b.hashCode() : true);
    }

    private static Gen<DataStorageSpec> gen()
    {
        Gen<DataStorageSpec.DataStorageUnit> unitGen = SourceDSL.arbitrary().enumValues(DataStorageSpec.DataStorageUnit.class);
        Gen<Long> valueGen = SourceDSL.longs().between(0, Long.MAX_VALUE);;
        Gen<DataStorageSpec> gen = rs -> new DataStorageSpec(valueGen.generate(rs), unitGen.generate(rs));
        return gen.describedAs(DataStorageSpec::toString);
    }
}

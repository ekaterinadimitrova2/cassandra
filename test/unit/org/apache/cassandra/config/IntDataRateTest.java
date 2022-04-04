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

public class IntDataRateTest
{
    @Test
    public void testOverflowingConversion()
    {
        assertThatThrownBy(() -> new IntDataRate("2147483648MiB/s")).isInstanceOf(ConfigurationException.class)
                                                                    .hasMessageContaining("Invalid data rate:2.147483648E9 mebibytes_per_second; " +
                                                                                          "value must be between 0 and 2147483647 in mebibytes per second");
        assertThatThrownBy(() -> IntDataRate.inMebibytesPerSecond(2147483648L)).isInstanceOf(ConfigurationException.class)
                                                                               .hasMessageContaining("Invalid data rate:2.147483648E9 mebibytes_per_second; " +
                                                                                                      "value must be between 0 and 2147483647 in mebibytes per second");

        assertThatThrownBy(() -> new IntDataRate((Integer.MAX_VALUE*1024L+1L) + "KiB/s")).isInstanceOf(ConfigurationException.class)
                                                                                         .hasMessageContaining("Invalid data rate:2.1474836470009766E9 kibibytes_per_second; " +
                                                                                                               "value must be between 0 and 2147483647 in mebibytes per second");
        assertThatThrownBy(() -> new IntDataRate((Integer.MAX_VALUE*1024L*1024+1L) + "B/s")).isInstanceOf(ConfigurationException.class)
                                                                                            .hasMessageContaining("Invalid data rate:2.147483647000001E9 bytes_per_second; " +
                                                                                                                  "value must be between 0 and 2147483647 in mebibytes per second");
        assertThatThrownBy(() -> IntDataRate.megabitsPerSecondInMebibytesPerSecond(2147483648L)).isInstanceOf(ConfigurationException.class)
                                                                                                .hasMessageContaining("Invalid data rate: 2147483648 megabits per second; " +
                                                                                                                      "stream_throughput_outbound and " +
                                                                                                                      "inter_dc_stream_throughput_outbound should " +
                                                                                                                      "be between 0 and 2147483647 in megabits per second");
    }

    @Test
    public void testValidUnits()
    {
        // we need toString as internally it is double and they are not 0.0 equal but for the end user the double numbers don't exist
        assertEquals(new IntDataRate("24MiB/s").toString(), IntDataRate.megabitsPerSecondInMebibytesPerSecond(200).toString());
    }
}

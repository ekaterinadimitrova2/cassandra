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

import org.apache.cassandra.exceptions.ConfigurationException;

import static org.apache.cassandra.config.DataRateSpec.DataRateUnit.MEBIBYTES_PER_SECOND;

/**
 * Represents a data rate int type used for cassandra configuration. It supports the opportunity for the users to be able to
 * add units to the confiuration parameter value. (CASSANDRA-15234)
 */
public class IntDataRate extends DataRateSpec
{
    public IntDataRate(String value)
    {
        super(value);

        // as we store in double and we don't have issues with precision we can afford this for int parameters
        // we chose mebibytes per second, int as the new streaming parameters added 4.1 were supposed to be int mebibytes
        validateQuantity(toMebibytesPerSecond(), this.unit());
    }

    public IntDataRate(double quantity, DataRateUnit unit)
    {
        super(quantity, unit);
    }

    public static IntDataRate megabitsPerSecondInMebibytesPerSecond(long megabitsPerSecond)
    {
        final double MEBIBYTES_PER_MEGABIT = 0.119209289550781;
        double mebibytesPerSecond = (double)megabitsPerSecond * MEBIBYTES_PER_MEGABIT;

        if (megabitsPerSecond > Integer.MAX_VALUE)
            throw new ConfigurationException("Invalid data rate: " + megabitsPerSecond +" megabits per second; " +
                                             "stream_throughput_outbound and inter_dc_stream_throughput_outbound" +
                                             " should be between 0 and " + Integer.MAX_VALUE + " in megabits per second");

        return new IntDataRate(mebibytesPerSecond, MEBIBYTES_PER_SECOND);
    }

    public static IntDataRate inMebibytesPerSecond(long mebibytesPerSecond)
    {
        validateQuantity(mebibytesPerSecond, MEBIBYTES_PER_SECOND);

        return new IntDataRate(mebibytesPerSecond, MEBIBYTES_PER_SECOND);
    }

    private static void validateQuantity(double quantity, DataRateUnit sourceUnit)
    {
        if (quantity > Integer.MAX_VALUE)
            throw new ConfigurationException("Invalid data rate:" + quantity + " " + sourceUnit.name().toLowerCase(Locale.ROOT) + "; value must be" +
                                             " between 0 and " + Integer.MAX_VALUE + " in mebibytes per second");
    }
}

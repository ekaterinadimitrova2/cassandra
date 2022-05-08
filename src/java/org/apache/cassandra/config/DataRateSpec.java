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

import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.primitives.Ints;

import org.apache.cassandra.exceptions.ConfigurationException;

import static org.apache.cassandra.config.DataRateSpec.DataRateUnit.BYTES_PER_SECOND;
import static org.apache.cassandra.config.DataRateSpec.DataRateUnit.MEBIBYTES_PER_SECOND;

/**
 * Represents a data rate type used for cassandra configuration. It supports the opportunity for the users to be able to
 * add units to the confiuration parameter value. (CASSANDRA-15234)
 */
public abstract class DataRateSpec
{
    /**
     * The Regexp used to parse the rate provided as String in cassandra.yaml.
     */
    private static final Pattern UNITS_PATTERN = Pattern.compile("^(\\d+)(MiB/s|KiB/s|B/s)$");

    private final double quantity;

    private final DataRateUnit unit;

    private DataRateSpec(String value)
    {
        //parse the string field value
        Matcher matcher = UNITS_PATTERN.matcher(value);

        if (!matcher.find())
            throw new ConfigurationException("Invalid data rate: " + value + " Accepted units: MiB/s, KiB/s, B/s where " +
                                             "case matters and " + "only non-negative values are valid");

        quantity = (double) Long.parseLong(matcher.group(1));
        unit = DataRateUnit.fromSymbol(matcher.group(2));
    }

    private DataRateSpec(String value, DataRateUnit smallestUnit, boolean isInt)
    {
        this (value);

        validateQuantity(value, this.quantity(), this.unit(), smallestUnit, isInt);

    }

    private DataRateSpec(double quantity, DataRateUnit unit, DataRateUnit smallestUnit, boolean isInt)
    {
        this.quantity = quantity;
        this.unit = unit;

        validateQuantity(quantity, unit, smallestUnit, isInt);
    }

    private static void validateQuantity(String value, double quantity, DataRateUnit unit, DataRateUnit smallestUnit, boolean isInt)
    {
        if (quantity < 0)
            throw new ConfigurationException("Invalid data rate: value must be non-negative");

        if (!isInt)
            validateLong(value, quantity, unit, smallestUnit);
        else
            validateInt(value, quantity, unit, smallestUnit);
    }

    private static void validateQuantity(double quantity, DataRateUnit unit, DataRateUnit smallestUnit, boolean isInt)
    {
        if (quantity < 0)
            throw new ConfigurationException("Invalid data rate: value must be non-negative");

        if (!isInt)
            validateLong(quantity, unit, smallestUnit);
        else
            validateInt(quantity, unit, smallestUnit);
    }

    private static void validateLong(String value, double quantity, DataRateUnit unit, DataRateUnit smallestUnit)
    {
        if (smallestUnit.convert(quantity, unit) >= Long.MAX_VALUE)
            throw new ConfigurationException("Invalid data rate: " + value + ". It shouldn't be more than " +
                                             (Long.MAX_VALUE - 1) + " in " + smallestUnit.name().toLowerCase());
    }

    private static void validateInt(String value, double quantity, DataRateUnit unit, DataRateUnit smallestUnit)
    {
        if (smallestUnit.convert(quantity, unit) > Integer.MAX_VALUE)
            throw new ConfigurationException("Invalid data rate: " + value + ". It shouldn't be more than " +
                                             Integer.MAX_VALUE + " in " + smallestUnit.name().toLowerCase());
    }

    private static void validateLong(double quantity, DataRateUnit unit, DataRateUnit smallestUnit)
    {
        if (smallestUnit.convert(quantity, unit) >= Long.MAX_VALUE)
            throw new ConfigurationException("Invalid data rate: " + quantity + " " + unit.name().toLowerCase() + ". It shouldn't be more than " +
                                             (Long.MAX_VALUE - 1) + " in " + smallestUnit.name().toLowerCase());
    }

    private static void validateInt(double quantity, DataRateUnit unit, DataRateUnit smallestUnit)
    {
        if (smallestUnit.convert(quantity, unit) > Integer.MAX_VALUE)
            throw new ConfigurationException("Invalid data rate: " + quantity + " " + unit.name().toLowerCase() + ". It shouldn't be more than " +
                                             Integer.MAX_VALUE + " in " + smallestUnit.name().toLowerCase());
    }

    /**
     * @return the data rate unit assigned.
     */
    public DataRateUnit unit()
    {
        return unit;
    }

    /**
     * @return the data rate quantity.
     */
    private double quantity()
    {
        return quantity;
    }

    /**
     * @return the data rate in bytes per second
     */
    public double toBytesPerSecond()
    {
        return unit.toBytesPerSecond(quantity);
    }

    /**
     * Returns the data rate in bytes per second as an {@code int}
     *
     * @return the data rate in bytes per second or {@code Integer.MAX_VALUE} if the rate is too large.
     */
    public int toBytesPerSecondAsInt()
    {
        return Ints.saturatedCast(Math.round(toBytesPerSecond()));
    }

    /**
     * @return the data rate in kibibytes per second
     */
    public double toKibibytesPerSecond()
    {
        return unit.toKibibytesPerSecond(quantity);
    }

    /**
     * Returns the data rate in kibibytes per second as an {@code int}
     *
     * @return the data rate in kibibytes per second or {@code Integer.MAX_VALUE} if the number of kibibytes is too large.
     */
    public int toKibibytesPerSecondAsInt()
    {
        return Ints.saturatedCast(Math.round(toKibibytesPerSecond()));
    }

    /**
     * @return the data rate in mebibytes per second
     */
    public double toMebibytesPerSecond()
    {
        return unit.toMebibytesPerSecond(quantity);
    }

    /**
     * Returns the data rate in mebibytes per second as an {@code int}
     *
     * @return the data rate in mebibytes per second or {@code Integer.MAX_VALUE} if the number of mebibytes is too large.
     */
    public int toMebibytesPerSecondAsInt()
    {
        return Ints.saturatedCast(Math.round(toMebibytesPerSecond()));
    }

    /**
     * This method is required in order to support backward compatibility with the old unit used for a few Data Rate
     * parameters before CASSANDRA-15234
     *
     * @return the data rate in megabits per second.
     */
    public double toMegabitsPerSecond()
    {
        return unit.toMegabitsPerSecond(quantity);
    }

    /**
     * Returns the data rate in megabits per second as an {@code int}. This method is required in order to support
     * backward compatibility with the old unit used for a few Data Rate parameters before CASSANDRA-15234
     *
     * @return the data rate in mebibytes per second or {@code Integer.MAX_VALUE} if the number of mebibytes is too large.
     */
    public int toMegabitsPerSecondAsInt()
    {
        return Ints.saturatedCast(Math.round(toMegabitsPerSecond()));
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(unit.toKibibytesPerSecond(quantity));
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;

        if (!(obj instanceof DataRateSpec))
            return false;

        DataRateSpec other = (DataRateSpec) obj;
        if (unit == other.unit)
            return quantity == other.quantity;

        // Due to overflows we can only guarantee that the 2 data rates are equal if we get the same results
        // doing the conversion in both directions.
        return unit.convert(other.quantity, other.unit) == quantity && other.unit.convert(quantity, unit) == other.quantity;
    }

    @Override
    public String toString()
    {
        return Math.round(quantity) + unit.symbol;
    }

    /**
     * Represents a data rate used for cassandra configuration. It supports the opportunity for the users to be able to
     * add units to the confiuration parameter value. The range is long bytes per second. (CASSANDRA-15234)
     */
    public final static class BytesPerSecond extends DataRateSpec
    {
        /**
         * Creates a {@code DataRateSpec.BytesPerSecond} of the specified amount.
         *
         * @param value the data rate
         */
        public BytesPerSecond(String value)
        {
            super(value, BYTES_PER_SECOND, false);
        }

        /**
         * Creates a {@code DataRateSpec.BytesPerSecond} of the specified amount in the specified unit.
         *
         * @param quantity where quantity shouldn't be bigger than Long.MAX_VALUE - 1 in bytes per second
         * @param unit     in which the provided quantity is
         */
        public BytesPerSecond(double quantity, DataRateUnit unit)
        {
            super(quantity, unit, BYTES_PER_SECOND, false);
        }

        /**
         * Creates a {@code DataRateSpec.BytesPerSecond} of the specified amount in bytes per second.
         *
         * @param quantityInBperS where quantityInBperS shouldn't be bigger than Long.MAX_VALUE
         */
        public BytesPerSecond(long quantityInBperS)
        {
            this(quantityInBperS, BYTES_PER_SECOND);
        }
    }

    /**
     * Represents a data rate int type used for cassandra configuration. It supports the opportunity for the users to be able to
     * add units to the confiuration parameter value. The range is Int mebibytes per second. (CASSANDRA-15234)
     */
    public final static class IntMebibytesPerSecond extends DataRateSpec
    {
        public IntMebibytesPerSecond(String value)
        {
            super(value, MEBIBYTES_PER_SECOND, true);
        }

        public IntMebibytesPerSecond(double quantity, DataRateUnit unit)
        {
            super(quantity, unit, MEBIBYTES_PER_SECOND, true);
        }

        // this one should be used only for backward compatibility
        public static IntMebibytesPerSecond megabitsPerSecondInMebibytesPerSecond(long megabitsPerSecond)
        {
            final double MEBIBYTES_PER_MEGABIT = 0.119209289550781;
            double mebibytesPerSecond = (double) megabitsPerSecond * MEBIBYTES_PER_MEGABIT;

            if (megabitsPerSecond > Integer.MAX_VALUE)
                throw new ConfigurationException("Invalid data rate: " + megabitsPerSecond + " megabits per second; " +
                                                 "stream_throughput_outbound and inter_dc_stream_throughput_outbound" +
                                                 " should be between 0 and " + Integer.MAX_VALUE + " in megabits per second");

            return new IntMebibytesPerSecond(mebibytesPerSecond, MEBIBYTES_PER_SECOND);
        }

        public IntMebibytesPerSecond(long mebibytesPerSecond)
        {
            this (mebibytesPerSecond, MEBIBYTES_PER_SECOND);
        }
    }

    public enum DataRateUnit
    {
        BYTES_PER_SECOND("B/s")
        {
            public double toBytesPerSecond(double d)
            {
                return d;
            }

            public double toKibibytesPerSecond(double d)
            {
                return d / 1024.0;
            }

            public double toMebibytesPerSecond(double d)
            {
                return d / (1024.0 * 1024.0);
            }

            public double toMegabitsPerSecond(double d)
            {
                return (d / 125000.0);
            }

            public double convert(double source, DataRateUnit sourceUnit)
            {
                return sourceUnit.toBytesPerSecond(source);
            }
        },
        KIBIBYTES_PER_SECOND("KiB/s")
        {
            public double toBytesPerSecond(double d)
            {
                return x(d, 1024.0, (MAX / 1024.0));
            }

            public double toKibibytesPerSecond(double d)
            {
                return d;
            }

            public double toMebibytesPerSecond(double d)
            {
                return d / 1024.0;
            }

            public double toMegabitsPerSecond(double d)
            {
                return d / 122.0;
            }

            public double convert(double source, DataRateUnit sourceUnit)
            {
                return sourceUnit.toKibibytesPerSecond(source);
            }
        },
        MEBIBYTES_PER_SECOND("MiB/s")
        {
            public double toBytesPerSecond(double d)
            {
                return x(d, (1024.0 * 1024.0), (MAX / (1024.0 * 1024.0)));
            }

            public double toKibibytesPerSecond(double d)
            {
                return x(d, 1024.0, (MAX / 1024.0));
            }

            public double toMebibytesPerSecond(double d)
            {
                return d;
            }

            public double toMegabitsPerSecond(double d)
            {
                if (d > MAX / (MEGABITS_PER_MEBIBYTE))
                    return MAX;
                return Math.round(d * MEGABITS_PER_MEBIBYTE);
            }

            public double convert(double source, DataRateUnit sourceUnit)
            {
                return sourceUnit.toMebibytesPerSecond(source);
            }
        };

        static final double MAX = Long.MAX_VALUE;
        static final double MEGABITS_PER_MEBIBYTE = 8.388608;

        /**
         * Scale d by m, checking for overflow. This has a short name to make above code more readable.
         */
        static double x(double d, double m, double over)
        {
            assert (over > 0.0) && (over < (MAX - 1)) && (over == (MAX / m));

            if (d > over)
                return MAX;
            return d * m;
        }

        /**
         * @param symbol the unit symbol
         * @return the rate unit corresponding to the given symbol
         */
        public static DataRateUnit fromSymbol(String symbol)
        {
            for (DataRateUnit value : values())
            {
                if (value.symbol.equalsIgnoreCase(symbol))
                    return value;
            }
            throw new ConfigurationException(String.format("Unsupported data rate unit: %s. Supported units are: %s",
                                                           symbol, Arrays.stream(values())
                                                                         .map(u -> u.symbol)
                                                                         .collect(Collectors.joining(", "))));
        }

        /**
         * The unit symbol
         */
        private final String symbol;

        DataRateUnit(String symbol)
        {
            this.symbol = symbol;
        }

        public double toBytesPerSecond(double d)
        {
            throw new AbstractMethodError();
        }

        public double toKibibytesPerSecond(double d)
        {
            throw new AbstractMethodError();
        }

        public double toMebibytesPerSecond(double d)
        {
            throw new AbstractMethodError();
        }

        public double toMegabitsPerSecond(double d)
        {
            throw new AbstractMethodError();
        }

        public double convert(double source, DataRateUnit sourceUnit)
        {
            throw new AbstractMethodError();
        }
    }
}

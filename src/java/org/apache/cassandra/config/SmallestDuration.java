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

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.exceptions.ConfigurationException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Wrapper class for Cassandra duration configuration parameters which are internally represented in Unit bigger than nanoseconds. In order
 * not to lose precision while converting to smaller units (until we migrate those parameters to use internally the smallest
 * supported unit) we restrict those parameters to use only the smallest provided or larger units. (CASSANDRA-15234)
 */
public abstract class SmallestDuration extends DurationSpec
{
    protected SmallestDuration(String value, TimeUnit smallestUnit, boolean isInt)
    {
        super(value, smallestUnit);

        validateQuantity(value, this.quantity(), this.unit(), smallestUnit, isInt);
    }

    protected SmallestDuration(long quantity, TimeUnit unit, TimeUnit smallestUnit, boolean isInt)
    {
        super(quantity, smallestUnit);

        validateQuantity(quantity, unit, smallestUnit, isInt);
    }

    protected SmallestDuration(double quantity, TimeUnit unit, TimeUnit smallestUnit, boolean isInt)
    {
        super(quantity, smallestUnit);

        // this constructor is used only for commitlog_sync_group_window_in_ms which was double pre-4.1, considered a bug now
        // internally it was casting to int but we keep things backward compatible on yaml level
        validateQuantity(Math.round(quantity), unit, smallestUnit, isInt);
    }

    private static void validateQuantity(String value, long quantity, TimeUnit unit, TimeUnit smallestUnit, boolean isInt)
    {
        if (!isInt)
            validateLong(value, quantity, unit, smallestUnit);
        else
            validateInt(value, quantity, unit, smallestUnit);
    }

    private static void validateQuantity(long quantity, TimeUnit unit, TimeUnit smallestUnit, boolean isInt)
    {
        if (!isInt)
            validateLong(quantity, unit, smallestUnit);
        else
            validateInt(quantity, unit, smallestUnit);
    }

    private static void validateLong(String value, long quantity, TimeUnit unit, TimeUnit smallestUnit)
    {
        if (smallestUnit.convert(quantity, unit) == Long.MAX_VALUE)
            throw new ConfigurationException("Invalid duration: " + value + ". It shouldn't be more than " +
                                             (Long.MAX_VALUE - 1) + " in " + smallestUnit.name().toLowerCase());
    }

    private static void validateInt(String value, long quantity, TimeUnit unit, TimeUnit smallestUnit)
    {
        if (smallestUnit.convert(quantity, unit) > Integer.MAX_VALUE)
            throw new ConfigurationException("Invalid duration: " + value + ". It shouldn't be more than " +
                                             Integer.MAX_VALUE + " in " + smallestUnit.name().toLowerCase());
    }

    private static void validateLong(long quantity, TimeUnit unit, TimeUnit smallestUnit)
    {
        if (smallestUnit.convert(quantity, unit) == Long.MAX_VALUE)
            throw new ConfigurationException("Invalid duration: " + quantity + " " + unit.name().toLowerCase() + ". It shouldn't be more than " +
                                             (Long.MAX_VALUE - 1) + " in " + smallestUnit.name().toLowerCase());
    }

    private static void validateInt(long quantity, TimeUnit unit, TimeUnit smallestUnit)
    {
        if (smallestUnit.convert(quantity, unit) > Integer.MAX_VALUE)
            throw new ConfigurationException("Invalid duration: " + quantity + " " + unit.name().toLowerCase() + ". It shouldn't be more than " +
                                             Integer.MAX_VALUE + " in " + smallestUnit.name().toLowerCase());
    }

    public static class Milliseconds extends SmallestDuration
    {
        /**
         * Creates a {@code SmallestDuration.Milliseconds} of the specified amount.
         *
         * @param value the duration
         *
         */
        public Milliseconds(String value)
        {
            super(value, MILLISECONDS, false);
        }

        /**
         * Creates a {@code SmallestDataStorage.Milliseconds} of the specified amount in the specified unit.
         *
         * @param quantity where quantity shouldn't be bigger than Long.MAX_VALUE - 1 in milliseconds
         * @param unit in which the provided quantity is
         */
        public Milliseconds(long quantity, TimeUnit unit)
        {
            super(quantity, unit, MILLISECONDS, false);
        }
    }

    public static class Seconds extends SmallestDuration
    {
        /**
         * Creates a {@code SmallestDuration.Seconds} of the specified amount.
         *
         * @param value the duration
         *
         */
        public Seconds(String value)
        {
            super(value, SECONDS, false);
        }

        /**
         * Creates a {@code SmallestDataStorage.Seconds} of the specified amount in the specified unit.
         *
         * @param quantity where quantity shouldn't be bigger than Long.MAX_VALUE - 1 in seconds
         * @param unit in which the provided quantity is
         */
        public Seconds(long quantity, TimeUnit unit)
        {
            super(quantity, unit, SECONDS, false);
        }
    }

    public static class Minutes extends SmallestDuration
    {
        /**
         * Creates a {@code SmallestDuration.Minutes} of the specified amount.
         *
         * @param value the duration
         *
         */
        public Minutes(String value)
        {
            super(value, MINUTES, false);
        }

        /**
         * Creates a {@code SmallestDataStorage.Minutes} of the specified amount in the specified unit.
         *
         * @param quantity where quantity shouldn't be bigger than Long.MAX_VALUE - 1 in minutes
         * @param unit in which the provided quantity is
         */
        public Minutes(long quantity, TimeUnit unit)
        {
            super(quantity, unit, MINUTES, false);
        }
    }

    public static class IntMilliseconds extends SmallestDuration
    {
        /**
         * Creates a {@code SmallestDuration.IntMilliseconds} of the specified amount which shouldn't be bigger than {@code Integer.MAX_VALUE}
         * in milliseconds
         * @param value the duration
         */
        public IntMilliseconds(String value)
        {
            super(value, MILLISECONDS, true);
        }

        /**
         * Creates a {@code SmallestDuration.IntMilliseconds} of the specified amount in the specified unit.
         *
         * @param quantity where quantity shouldn't be bigger than Integer.MAX_VALUE in milliseconds
         * @param unit in which the provided quantity is
         */
        public IntMilliseconds(long quantity, TimeUnit unit)
        {
            super(quantity, unit, MILLISECONDS, true);
        }

        /**
         * Creates a {@code SmallestDuration.IntMilliseconds} of the specified amount in milliseconds.
         *
         * @param quantityInMs where quantityInMs shouldn't be bigger than Integer.MAX_VALUE
         */
        public IntMilliseconds(long quantityInMs)
        {
            this(quantityInMs, MILLISECONDS);
        }

        /**
         * Below constructor is used only for backward compatibility for the old commitlog_sync_group_window_in_ms before 4.1
         * Creates a {@code SmallestDataStorage.Milliseconds} of the specified amount in the specified unit.
         *
         * @param quantity where quantity shouldn't be bigger than Long.MAX_VALUE - 1 in milliseconds
         * @param unit in which the provided quantity is
         */
        public IntMilliseconds(double quantity, TimeUnit unit)
        {
            super(quantity, unit, MILLISECONDS, false);
        }
    }

    public static class IntSeconds extends SmallestDuration
    {
        /**
         * Creates a {@code SmallestDuration.IntSeconds} of the specified amount which shouldn't be bigger than {@code Integer.MAX_VALUE}
         * in seconds
         * @param value the duration
         */
        public IntSeconds(String value)
        {
            super(value, MILLISECONDS, true);
        }

        /**
         * Creates a {@code SmallestDuration.IntSeconds} of the specified amount in the specified unit.
         *
         * @param quantity where quantity shouldn't be bigger than Integer.MAX_VALUE in seconds
         * @param unit in which the provided quantity is
         */
        public IntSeconds(long quantity, TimeUnit unit)
        {
            super(quantity, unit, SECONDS, true);
        }

        /**
         * Creates a {@code SmallestDuration.IntSeconds} of the specified amount in seconds.
         *
         * @param quantityInS where quantityInS shouldn't be bigger than Integer.MAX_VALUE
         */
        public IntSeconds(long quantityInS)
        {
            this(quantityInS, SECONDS);
        }
    }

    public static class IntMinutes extends SmallestDuration
    {
        /**
         * Creates a {@code SmallestDuration.IntMinutes} of the specified amount which shouldn't be bigger than {@code Integer.MAX_VALUE}
         * in minutes
         * @param value the duration
         */
        public IntMinutes(String value)
        {
            super(value, MINUTES, true);
        }

        /**
         * Creates a {@code SmallestDuration.IntMinutes} of the specified amount in the specified unit.
         *
         * @param quantity where quantity shouldn't be bigger than Integer.MAX_VALUE in minutes
         * @param unit in which the provided quantity is
         */
        public IntMinutes(long quantity, TimeUnit unit)
        {
            super(quantity, unit, MINUTES, true);
        }

        /**
         * Creates a {@code SmallestDuration.IntMinutes} of the specified amount in minutes.
         *
         * @param quantityInM where quantityInM shouldn't be bigger than Integer.MAX_VALUE
         */
        public IntMinutes(long quantityInM)
        {
            this(quantityInM, MINUTES);
        }
    }
}

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


import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Wrapper class for Cassandra data storage configuration parameters which are internally represented in Unit bigger than byte. In order
 * not to lose precision while converting to smaller units (until we migrate those parameters to use internally the smallest
 * supported unit) we restrict those parameters to use only the smallest provided or larger units. (CASSANDRA-15234)
 */
public abstract class SmallestDataStorage extends DataStorageSpec
{
    protected SmallestDataStorage(String value, DataStorageUnit smallestUnit, boolean isInt)
    {
        super(value, smallestUnit);

        validateQuantity(value, this.quantity(), this.unit(), smallestUnit, isInt);
    }

    protected SmallestDataStorage(long quantity, DataStorageUnit unit, DataStorageUnit smallestUnit, boolean isInt)
    {
        super(quantity, unit);

        validateQuantity(quantity, unit, smallestUnit, isInt);
    }

    private static void validateQuantity(String value, long quantity, DataStorageUnit unit, DataStorageUnit smallestUnit, boolean isInt)
    {
        if (!isInt)
            validateLong(value, quantity, unit, smallestUnit);
        else
            validateInt(value, quantity, unit, smallestUnit);
    }

    private static void validateQuantity(long quantity, DataStorageUnit unit, DataStorageUnit smallestUnit, boolean isInt)
    {
        if (!isInt)
            validateLong(quantity, unit, smallestUnit);
        else
            validateInt(quantity, unit, smallestUnit);
    }

    private static void validateLong(String value, long quantity, DataStorageUnit unit, DataStorageUnit smallestUnit)
    {
        if (smallestUnit.convert(quantity, unit) == Long.MAX_VALUE)
            throw new ConfigurationException("Invalid data storage: " + value + ". It shouldn't be more than " +
                                             (Long.MAX_VALUE - 1) + " in " + smallestUnit.name().toLowerCase());
    }

    private static void validateInt(String value, long quantity, DataStorageUnit unit, DataStorageUnit smallestUnit)
    {
        if (smallestUnit.convert(quantity, unit) > Integer.MAX_VALUE)
            throw new ConfigurationException("Invalid data storage: " + value + ". It shouldn't be more than " +
                                             Integer.MAX_VALUE + " in " + smallestUnit.name().toLowerCase());
    }

    private static void validateLong(long quantity, DataStorageUnit unit, DataStorageUnit smallestUnit)
    {
        if (smallestUnit.convert(quantity, unit) == Long.MAX_VALUE)
            throw new ConfigurationException("Invalid data storage: " + quantity + " " + unit.name().toLowerCase() + ". It shouldn't be more than " +
                                             (Long.MAX_VALUE - 1) + " in " + smallestUnit.name().toLowerCase());
    }

    private static void validateInt(long quantity, DataStorageUnit unit, DataStorageUnit smallestUnit)
    {
        if (smallestUnit.convert(quantity, unit) > Integer.MAX_VALUE)
            throw new ConfigurationException("Invalid data storage: " + quantity + " " + unit.name().toLowerCase() + ". It shouldn't be more than " +
                                             Integer.MAX_VALUE + " in " + smallestUnit.name().toLowerCase());
    }

    public static class Kibibytes extends SmallestDataStorage
    {
        /**
         * Creates a {@code SmallestDataStorage.Kibibytes} of the specified amount.
         *
         * @param value the data storage
         *
         */
        public Kibibytes(String value)
        {
            super(value, DataStorageUnit.KIBIBYTES, false);
        }

        /**
         * Creates a {@code SmallestDataStorage.Kibibytes} of the specified amount in the specified unit.
         *
         * @param quantity where quantity shouldn't be bigger than Long.MAX_VALUE - 1 in kibibytes
         * @param unit in which the provided quantity is
         */
        public Kibibytes(long quantity, DataStorageUnit unit)
        {
            super(quantity, unit, DataStorageUnit.KIBIBYTES, false);
        }
    }

    public static class Mebibytes extends SmallestDataStorage
    {
        /**
         * Creates a {@code SmallestDataStorage.Mebibytes} of the specified amount.
         *
         * @param value the data storage
         */
        public Mebibytes(String value)
        {
            super(value, DataStorageUnit.MEBIBYTES, false);
        }

        /**
         * Creates a {@code SmallestDataStorage.Mebibytes} of the specified amount in the specified unit.
         *
         * @param quantity where quantity shouldn't be bigger than Long.MAX_VALUE - 1 in mebibytes
         * @param unit in which the provided quantity is
         */
        public Mebibytes(long quantity, DataStorageUnit unit)
        {
            super(quantity, unit, DataStorageUnit.MEBIBYTES, false);
        }

        /**
         * Creates a {@code SmallestDataStorage.Mebibytes} of the specified amount in mebibytes.
         *
         * @param quantityInMiB where quantityInMiB shouldn't be bigger than Integer.MAX_VALUE
         */
        public Mebibytes(long quantityInMiB)
        {
            this(quantityInMiB, DataStorageUnit.MEBIBYTES);
        }
    }

    public static class IntBytes extends SmallestDataStorage
    {
        /**
         * Creates a {@code SmallestDataStorage.IntBytes} of the specified amount which shouldn't be bigger than {@code Integer.MAX_VALUE}
         * in bytes
         * @param value the data storage
         */
        public IntBytes(String value)
        {
            super(value, DataStorageUnit.BYTES, true);
        }

        /**
         * Creates a {@code SmallestDataStorage.IntBytes} of the specified amount in the specified unit.
         *
         * @param quantity where quantity shouldn't be bigger than Integer.MAX_VALUE in bytes
         * @param unit in which the provided quantity is
         */
        public IntBytes(long quantity, DataStorageUnit unit)
        {
            super(quantity, unit, DataStorageUnit.BYTES, true);
        }

        /**
         * Creates a {@code SmallestDataStorage.IntBytes} of the specified amount in bytes.
         *
         * @param quantityInB where quantityInB shouldn't be bigger than Integer.MAX_VALUE
         */
        public IntBytes(long quantityInB)
        {
            this(quantityInB, DataStorageUnit.BYTES);
        }
    }

    public static class IntKibibytes extends SmallestDataStorage
    {
        /**
         * Creates a {@code SmallestDataStorage.IntKibibytes} of the specified amount which shouldn't be bigger than {@code Integer.MAX_VALUE}
         * in kibibytes
         * @param value the data storage
         */
        public IntKibibytes(String value)
        {
            super(value, DataStorageUnit.KIBIBYTES, true);
        }

        /**
         * Creates a {@code SmallestDataStorage.IntKibibytes} of the specified amount in the specified unit.
         *
         * @param quantity where quantity shouldn't be bigger than Integer.MAX_VALUE in kibibytes
         * @param unit in which the provided quantity is
         */
        public IntKibibytes(long quantity, DataStorageUnit unit)
        {
            super(quantity, unit, DataStorageUnit.KIBIBYTES, true);
        }

        /**
         * Creates a {@code SmallestDataStorage.IntKibibytes} of the specified amount in kibibytes.
         *
         * @param quantityInKiB where quantityInKiB shouldn't be bigger than Integer.MAX_VALUE
         */
        public IntKibibytes(long quantityInKiB)
        {
            this(quantityInKiB, DataStorageUnit.KIBIBYTES);
        }
    }

    public static class IntMebibytes extends SmallestDataStorage
    {
        /**
         * Creates a {@code SmallestDataStorage.IntMebibytes} of the specified amount which shouldn't be bigger than {@code Integer.MAX_VALUE}
         * in mebibytes
         * @param value the data storage
         */
        public IntMebibytes(String value)
        {
            super(value, DataStorageUnit.MEBIBYTES, true);
        }

        /**
         * Creates a {@code SmallestDataStorage.IntMebibytes} of the specified amount in the specified unit.
         *
         * @param quantity where quantity shouldn't be bigger than Integer.MAX_VALUE in mebibytes
         * @param unit in which the provided quantity is
         */
        public IntMebibytes(long quantity, DataStorageUnit unit)
        {
            super(quantity, unit, DataStorageUnit.MEBIBYTES, true);
        }

        /**
         * Creates a {@code SmallestDataStorage.IntMebibytes} of the specified amount in mebibytes.
         *
         * @param quantityInMiB where quantityInMiB shouldn't be bigger than Integer.MAX_VALUE
         */
        public IntMebibytes(long quantityInMiB)
        {
            this(quantityInMiB, DataStorageUnit.MEBIBYTES);
        }
    }
}

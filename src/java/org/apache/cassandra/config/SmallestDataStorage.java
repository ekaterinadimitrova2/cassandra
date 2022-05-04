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

import java.util.function.LongSupplier;

import org.apache.cassandra.exceptions.ConfigurationException;

public abstract class SmallestDataStorage extends DataStorageSpec
{
    protected SmallestDataStorage(String value, DataStorageUnit unit)
    {
        super(value, unit);
    }

    protected SmallestDataStorage(long quantity, DataStorageUnit unit)
    {
        super(quantity, unit);
    }

    protected static void validateLong(String value, LongSupplier valueSupplier, DataStorageUnit unit)
    {
        if (valueSupplier.getAsLong() == Long.MAX_VALUE)
            throw new ConfigurationException("Invalid data storage: " + value + ". It shouldn't be more than" +
                                             (Long.MAX_VALUE - 1) + " in " + unit.name().toLowerCase());
    }

    protected static void validateInt(LongSupplier valueSupplier, DataStorageUnit unit)
    {
        validateInt(valueSupplier.getAsLong(), unit);
    }

    protected static void validateInt(long value, DataStorageUnit unit)
    {
        if (value > Integer.MAX_VALUE)
            throw new ConfigurationException("Invalid data storage: values must be less than " + Integer.MAX_VALUE +
                                             " "  + unit.name().toLowerCase() + ", but it was " + value + " " + unit.name().toLowerCase());
    }

    public static class Kibibytes extends SmallestDataStorage
    {
        public Kibibytes(String value)
        {
            super(value, DataStorageUnit.KIBIBYTES);
            validateLong(value, this::toKibibytes, DataStorageUnit.KIBIBYTES);
        }

        public Kibibytes(long quantity, DataStorageUnit unit)
        {
            super(quantity, unit);
        }
    }

    public static class Mebibytes extends SmallestDataStorage
    {
        public Mebibytes(String value)
        {
            super(value, DataStorageUnit.MEBIBYTES);
            validateLong(value, this::toMebibytes, DataStorageUnit.MEBIBYTES);
        }

        public Mebibytes(long quantity, DataStorageUnit unit)
        {
            super(quantity, unit);
        }
    }

    public static class IntBytes extends SmallestDataStorage
    {
        public IntBytes(String value)
        {
            super(value, DataStorageUnit.BYTES);
            validateInt(this::toBytes, DataStorageUnit.BYTES);
        }

        public IntBytes(long quantity, DataStorageUnit unit)
        {
            super(quantity, unit);
            validateInt(this::toBytes, DataStorageUnit.BYTES);
        }

        public IntBytes(long quantityInBytes)
        {
            this(quantityInBytes, DataStorageUnit.BYTES);
        }
    }

    public static class IntKibibytes extends SmallestDataStorage
    {
        public IntKibibytes(String value)
        {
            super(value, DataStorageUnit.KIBIBYTES);
            validateInt(this::toKibibytes, DataStorageUnit.KIBIBYTES);
        }

        public IntKibibytes(long quantity, DataStorageUnit unit)
        {
            super(quantity, unit);
            validateInt(this::toKibibytes, DataStorageUnit.KIBIBYTES);
        }

        public IntKibibytes(long quantityInKiB)
        {
            this(quantityInKiB, DataStorageUnit.KIBIBYTES);
        }
    }

    public static class IntMebibytes extends SmallestDataStorage
    {
        public IntMebibytes(String value)
        {
            super(value, DataStorageUnit.MEBIBYTES);
            validateInt(this::toMebibytes, DataStorageUnit.MEBIBYTES);
        }

        public IntMebibytes(long quantity, DataStorageUnit unit)
        {
            super(quantity, unit);
            validateInt(this::toMebibytes, DataStorageUnit.MEBIBYTES);
        }

        public IntMebibytes(long quantityInMiB)
        {
            this(quantityInMiB, DataStorageUnit.MEBIBYTES);
        }
    }
}

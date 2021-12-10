/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.config;

/**
 * An interface to support conversions for backward compatibility with the old cassandra.yaml where duration, bit rate and
 * data storage configuration parameters were provided only by value and the expected unit was part of the configuration
 * parameter name. (CASSANDRA-15234)
 * It is important to be noted that this converter is not intended to be used when we don't change name of a configuration
 * parameter but we want to add unit. This would always default to the old value provided without a unit at the moment.
 * in case this functionality is needed at some point, please, raise a ticket.
 */
public interface Converter<Original, Current>
{
    /**
     * A method to identify what type is needed to be returned from the conversion used for a configuration parameter
     * in {@link Replaces} annotation in {@link Config}
     * @return class type
     */
    Class<Original> getInputType();

    /**
     * Apply the converter specified as part of the {@link Replaces} annotation in {@link Config}
     * @param value we will use from cassandra.yaml to create a new {@link Config} parameter of type {@link DurationSpec},
     * {@link DataRateSpec} or {@link DataStorageSpec}
     * @return new object of type {@link DurationSpec}, {@link DataRateSpec} or {@link DataStorageSpec}
     */
    Current apply(Original value);

    /**
     * This converter is used when we change the name of a cassandra.yaml configuration parameter but we want to be
     * able to still use the old name too. No units involved.
     */
    public static final class IdentityConverter implements Converter<Object, Object>
    {
        public Class<Object> getInputType()
        {
            return null; // null means 'unchanged'  mostly used for renames
        }

        public Object apply(Object value)
        {
            return value;
        }
    }

    public static final class MillisDurationConverter implements Converter<Long, DurationSpec>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public DurationSpec apply(Long value)
        {
            if (value == null)
                return null;
            return DurationSpec.inMilliseconds(value);
        }
    }

    public static final class MillisDurationInDoubleConverter implements Converter<Double, DurationSpec>
    {

        public Class<Double> getInputType()
        {
            return Double.class;
        }

        public DurationSpec apply(Double value)
        {
            if (value == null)
                return null;
            return DurationSpec.inMilliseconds((long)value.doubleValue());
        }
    }

    public static final class MillisDurationConverterCustom implements Converter<Long, DurationSpec>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public DurationSpec apply(Long value)
        {
            if (value == null)
                return null;

            if (value.equals((long) -1))
                value = 0L;

            return DurationSpec.inMilliseconds(value);
        }
    }

    public static final class SecondsDurationConverter implements Converter<Long, DurationSpec>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public DurationSpec apply(Long value)
        {
            if (value == null)
                return null;
            return DurationSpec.inSeconds(value);
        }
    }

    public static final class MinutesDurationConverter implements Converter<Long, DurationSpec>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public DurationSpec apply(Long value)
        {
            if (value == null)
                return null;
            return DurationSpec.inMinutes(value);
        }
    }

    public static final class MegabytesDataStorageConverter implements Converter<Long, DataStorageSpec>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public DataStorageSpec apply(Long value)
        {
            if (value == null)
                return null;
            return DataStorageSpec.inMegabytes(value);
        }
    }

    public static final class KilobytesDataStorageConverter implements Converter<Long, DataStorageSpec>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public DataStorageSpec apply(Long value)
        {
            if (value == null)
                return null;
            return DataStorageSpec.inKilobytes(value);
        }
    }

    public static final class BytesDataStorageConverter implements Converter<Long, DataStorageSpec>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public DataStorageSpec apply(Long value)
        {
            if (value == null)
                return null;
            return DataStorageSpec.inBytes(value);
        }
    }

    public static final class MegabitsPerSecondDataRateConverter implements Converter<Long, DataRateSpec>
    {
        public Class<Long> getInputType()
        {
            return Long.class;
        }

        public DataRateSpec apply(Long value)
        {
            if (value == null)
                return null;
            return DataRateSpec.inMebibytesPerSecond(value);
        }
    }
}

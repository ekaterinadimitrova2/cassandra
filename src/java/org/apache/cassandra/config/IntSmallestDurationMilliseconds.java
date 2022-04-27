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

/**
 * Represents duration for Int bounded config which cannot be anything smaller than milliseconds
 */
public class IntSmallestDurationMilliseconds extends DurationSpec
{
    public IntSmallestDurationMilliseconds(String value)
    {
        super(value, TimeUnit.MILLISECONDS);

        if (value != null)
        {
            long milliseconds = toMilliseconds();
            if (milliseconds > Integer.MAX_VALUE)
                throw new ConfigurationException("Invalid duration: values must be less than " + Integer.MAX_VALUE +
                                                 " milliseconds, but it was " + milliseconds + " milliseconds");
        }
    }

    public IntSmallestDurationMilliseconds(long milliseconds, TimeUnit unit)
    {
        super(milliseconds, unit);

        if (milliseconds > Integer.MAX_VALUE)
            throw new ConfigurationException("Invalid duration: values must be less than " + Integer.MAX_VALUE +
                                             " milliseconds, but it was " + milliseconds + " milliseconds");
    }
    // TO DO As int methods and whatever else is needed
    /**
     * Creates a {@code IntSmallestDurationMilliseconds} of the specified amount of milliseconds.
     *
     * @param milliseconds the amount of milliseconds
     * @return a duration
     */
    public static IntSmallestDurationMilliseconds inMilliseconds(long milliseconds)
    {
        return new IntSmallestDurationMilliseconds(milliseconds, TimeUnit.MILLISECONDS);
    }
}

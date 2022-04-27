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
 * Represents data rate for Int bounded config
 */
public final class IntSmallestDataRateBytes extends DataRateSpec
{
    public IntSmallestDataRateBytes(String value)
    {
        super(value);

        if (value != null)
        {
            double bytespersecond = toBytesPerSecond();
            if (bytespersecond > Integer.MAX_VALUE)
                throw new ConfigurationException("Invalid data rate: values must be less than " + Integer.MAX_VALUE +
                                                 " B/s, but it was " + bytespersecond + " B/s");
        }
    }
    // TO DO As int methods and whatever else is needed
}
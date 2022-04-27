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
 * Represents an amount of data storage for Int bounded config which cannot be anything smaller than kibibytes
 */
public final class IntSmallestDataStorageKibibytes extends DataStorageSpec
{
    public IntSmallestDataStorageKibibytes(String value)
    {
        super(value, DataStorageSpec.DataStorageUnit.KIBIBYTES);

        if (value != null)
        {
            long kibibytes = toKibibytes();
            if (kibibytes > Integer.MAX_VALUE)
                throw new ConfigurationException("Invalid data storage: values must be less than " + Integer.MAX_VALUE +
                                                 "kibibytes, but it was " + kibibytes + "kibibytes");
        }
    }
    // TO DO As int methods and whatever else is needed
}

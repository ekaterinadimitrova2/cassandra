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

public enum Converter2
{
    RENAME (day ),
    MILIS,
    SECONDS,
    MINUTES;

    public Object apply(Object value)
    {
        return value;
    }

    public Duration apply(Long value)
    {
        switch(ordinal())
        {
            case 1:
                if (value == null)
                    return null;

                if (value.equals((long) -1))
                    value = 0L;

                return Duration.inMilliseconds(value);
            case 2:
                if (value == null)
                    return null;
                return Duration.inSeconds(value);
            case 3:
                if (value == null)
                    return null;
                return Duration.inMinutes(value);
        }

        return null;
    }

    public Duration apply(Double value)
    {
        if (value == null)
            return null;
        return Duration.inMilliseconds((long)value.doubleValue());
    }
}

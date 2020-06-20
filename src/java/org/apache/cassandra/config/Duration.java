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

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.primitives.Ints;

/**
 * Represents a positive time duration.
 */
public final class Duration
{
    /**
     * The Regexp used to parse the duration provided as String.
     */
    private static final Pattern TIME_UNITS_PATTERN = Pattern.compile(("^(\\d+)([a-zA-Z]{1,2}|µs|µS)$"));
    private static final Pattern DOUBLE_TIME_UNITS_PATTERN = Pattern.compile(("^(\\d+\\.\\d+)([a-zA-Z]{1,2}|µs|µS)$"));
    
    private final long quantity;

    private final TimeUnit unit;

    private static final String content = Duration.readStorageConfig(YamlConfigurationLoader.getStorageConfigURL());

    /**
     * 
     */

    public Duration(String value)
    {
        isConfigFileValid();

        if (value == null || value.equals("null"))
        {
            quantity = 0;
            unit = TimeUnit.MILLISECONDS;
            return;
        }

        //parse the string field value
        Matcher matcher = TIME_UNITS_PATTERN.matcher(value);
        Matcher matcherDouble = DOUBLE_TIME_UNITS_PATTERN.matcher(value);

        if(matcher.find())
        {
            quantity = Long.parseLong(matcher.group(1));
            unit = fromSymbol(matcher.group(2));
        }
        else if(matcherDouble.find())
        {
            quantity =(long) Double.parseDouble(matcherDouble.group(1));
            unit = fromSymbol(matcherDouble.group(2));
        }
        else {
            throw new IllegalArgumentException("Invalid duration: " + value);
        }
    }

    private static String readStorageConfig(URL url)
    {
        String content = "";

        try
        {
            content = new String (Files.readAllBytes(Paths.get(String.valueOf(url).substring(5))));

        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return content;
    }

    private Duration(long quantity, TimeUnit unit)
    {
        if (quantity < 0)
            throw new IllegalArgumentException("Duration must be positive");

        this.quantity = quantity;
        this.unit = unit;
    }

    private Duration(double quantity, TimeUnit unit)
    {
        if (quantity < 0)
            throw new IllegalArgumentException("Duration must be positive");

        this.quantity = (long) quantity;
        this.unit = unit;
    }

    /**
     * Creates a {@code Duration} of the specified amount of milliseconds.
     *
     * @param milliseconds the amount of milliseconds
     * @return a duration
     */
    public static Duration inMilliseconds(long milliseconds)
    {
        isConfigFileValid();

        return new Duration(milliseconds, TimeUnit.MILLISECONDS);
    }

    public static Duration inDoubleMilliseconds(double milliseconds)
    {
        isConfigFileValid();

        return new Duration(milliseconds, TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a {@code Duration} of the specified amount of seconds.
     *
     * @param seconds the amount of seconds
     * @return a duration
     */
    public static Duration inSeconds(long seconds)
    {
        isConfigFileValid();

        return new Duration(seconds, TimeUnit.SECONDS);
    }

    /**
     * Creates a {@code Duration} of the specified amount of minutes.
     *
     * @param minutes the amount of minutes
     * @return a duration
     */
    public static Duration inMinutes(long minutes)
    {
        isConfigFileValid();

        return new Duration(minutes, TimeUnit.MINUTES);
    }

    private static void isConfigFileValid()
    {
        if (isBlank("commitlog_sync_period") || isBlank("commitlog_sync_period"))
        {
            throw new IllegalArgumentException("You should provide a value for commitlog_sync_period or comment it in " +
                                               "order to get a default one");
        }

        if (isBlank("commitlog_sync_group_window") || isBlank("commitlog_sync_group_window"))
        {
            throw new IllegalArgumentException("You should provide a value for commitlog_sync_group_window or comment it in " +
                                               "order to get a default one");
        }

        if (isBlank("commitlog_sync_batch_window") || isBlank("commitlog_sync_batch_window"))
        {
            throw new IllegalArgumentException("You should provide a value for commitlog_sync_batch_window or comment it in " +
                                               "order to get a default one");
        }
    }

    private static boolean isBlank(String property)
    {
        Pattern p = Pattern.compile(String.format("%s%s *: *$", '^', property), Pattern.MULTILINE);
        Matcher m = p.matcher(Duration.content);
        return m.find();
    }

    /**
     * Returns the time unit associated to the specified symbol
     *
     * @param symbol the time unit symbol
     * @return the time unit associated to the specified symbol
     */
    private TimeUnit fromSymbol(String symbol)
    {
        switch (symbol.toLowerCase())
        {
            case "d": return TimeUnit.DAYS;
            case "h": return TimeUnit.HOURS;
            case "m": return TimeUnit.MINUTES;
            case "s": return TimeUnit.SECONDS;
            case "ms": return TimeUnit.MILLISECONDS;
            case "us":
            case "µs": return TimeUnit.MICROSECONDS;
            case "ns": return TimeUnit.NANOSECONDS;
        }
        throw new IllegalArgumentException(String.format("Unsupported time unit: %s. Supported units are: %s",
                                                         symbol, Arrays.stream(TimeUnit.values())
                                                                       .map(Duration::getSymbol)
                                                                       .collect(Collectors.joining(", "))));
    }

    /**
     * Returns this duration in the specified time unit
     *
     * @param targetUnit the time unit
     * @return this duration in the specified time unit
     */
    public long to(TimeUnit targetUnit)
    {
        return targetUnit.convert(quantity, unit);
    }

    /**
     * Returns this duration in number of minutes
     *
     * @return this duration in number of minutes
     */
    public long toMinutes()
    {
        return unit.toMinutes(quantity);
    }

    /**
     * Returns this duration in number of minutes as an {@code int}
     *
     * @return this duration in number of minutes or {@code Integer.MAX_VALUE} if the number of minutes is too large.
     */
    public int toMinutesAsInt()
    {
        return Ints.saturatedCast(toMinutes());
    }

    /**
     * Returns this duration in number of seconds
     *
     * @return this duration in number of seconds
     */
    public long toSeconds()
    {
        return unit.toSeconds(quantity);
    }

    /**
     * Returns this duration in number of seconds as an {@code int}
     *
     * @return this duration in number of seconds or {@code Integer.MAX_VALUE} if the number of seconds is too large.
     */
    public int toSecondsAsInt()
    {
        return Ints.saturatedCast(toSeconds());
    }

    /**
     * Returns this duration in number of milliseconds
     *
     * @return this duration in number of milliseconds
     */
    public long toMilliseconds()
    {
        return unit.toMillis(quantity);
    }

    /**
     * Returns this duration in number of milliseconds as an {@code int}
     *
     * @return this duration in number of milliseconds or {@code Integer.MAX_VALUE} if the number of milliseconds is too large.
     */
    public int toMillisecondsAsInt()
    {
        return Ints.saturatedCast(toMilliseconds());
    }

    @Override
    public int hashCode()
    {
        // Milliseconds seems to be a reasonable tradeoff
        return Objects.hash(unit.toMillis(quantity));
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;

        if (!(obj instanceof Duration))
            return false;

        Duration other = (Duration) obj;
        if (unit == other.unit)
            return quantity == other.quantity;

        // Due to overflows we can only guarantee that the 2 durations are equal if we get the same results
        // doing the convertion in both directions.
        return unit.convert(other.quantity, other.unit) == quantity && other.unit.convert(quantity, unit) == other.quantity;
    }

    @Override
    public String toString()
    {
        return quantity + getSymbol(unit);
    }

    /**
     * Returns the symbol associated to the specified unit
     *
     * @param unit the time unit
     * @return the time unit symbol
     */
    private static String getSymbol(TimeUnit unit)
    {
        switch (unit)
        {
            case DAYS: return "d";
            case HOURS: return "h";
            case MINUTES: return "m";
            case SECONDS: return "s";
            case MILLISECONDS: return "ms";
            case MICROSECONDS: return "us";
            case NANOSECONDS: return "ns";
        }
        throw new AssertionError();
    }
}

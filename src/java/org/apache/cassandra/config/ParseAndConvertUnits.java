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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * A class that parses the units suffixes(units) of all Duration, Memory, and Rate cassandra.yaml parameters.
 * It converts, if needed, the respective parameters' values to be in the default cassandra parameter's unit.
 */
public class ParseAndConvertUnits
{
    private static final Logger logger = LoggerFactory.getLogger(Config.class);

    /**
     * The Regexp used to parse the duration provided as String.
     */
    private static final Pattern TIME_UNITS_PATTERN =
    Pattern.compile(
    "(\\d+)(d|D|h|H|s|S|ms|MS|mS|Ms|us|US|uS|Us|µs|µS|ns|NS|nS|Nsm|M|m)");
    private static final Pattern DOUBLE_TIME_UNITS_PATTERN =
    Pattern.compile(
    "(\\d+\\.\\d+)(d|D|h|H|s|S|ms|MS|mS|Ms|us|US|uS|Us|µs|µS|ns|NS|nS|Nsm|M|m)");

    /**
     * The Regexp used to parse the memory provided as String.
     */
    private static final Pattern MEMORY_UNITS_PATTERN =
    Pattern.compile(
    "(\\d+)(kb|KB|mb|MB|gb|GB|b|B)");

    /**
     * The Regexp used to parse the rates provided as String.
     */
    private static final Pattern RATE_UNITS_PATTERN =
    Pattern.compile(
    "(\\d+)(bps|mbps|kbps|Kbps|Mbps)");

    private static final Map<String, String[]> DURATION_UNITS_MAP = new HashMap<String, String[]>()
    {
        {
            put("max_hint_window", new String[]{"max_hint_window_in_ms", "ms"});
            put("native_transport_idle_timeout", new String[]{"native_transport_idle_timeout_in_ms", "ms"});
            put("request_timeout", new String[]{"request_timeout_in_ms", "ms"});
            put("read_request_timeout", new String[]{"read_request_timeout_in_ms", "ms"});
            put("range_request_timeout", new String[]{"range_request_timeout_in_ms", "ms"});
            put("write_request_timeout", new String[]{"write_request_timeout_in_ms", "ms"});
            put("counter_write_request_timeout", new String[]{"counter_write_request_timeout_in_ms", "ms"});
            put("cas_contention_timeout", new String[]{"cas_contention_timeout_in_ms", "ms"});
            put("truncate_request_timeout", new String[]{"truncate_request_timeout_in_ms", "ms"});
            put("streaming_keep_alive_period", new String[]{"streaming_keep_alive_period_in_secs", "s"});
            put("slow_query_log_timeout", new String[]{"slow_query_log_timeout_in_ms", "ms"});
            put("internode_tcp_connect_timeout", new String[]{"internode_tcp_connect_timeout_in_ms", "ms"});
            put("internode_tcp_user_timeout", new String[]{"internode_tcp_user_timeout_in_ms", "ms"});
            put("commitlog_sync_batch_window", new String[]{"commitlog_sync_batch_window_in_ms", "ms"});
            put("commitlog_sync_group_window", new String[]{"commitlog_sync_group_window_in_ms", "ms"});
            put("commitlog_sync_period", new String[]{"commitlog_sync_period_in_ms", "ms"});
            put("periodic_commitlog_sync_lag_block", new String[]{"periodic_commitlog_sync_lag_block_in_ms", "ms"});
            put("cdc_free_space_check_interval", new String[]{"cdc_free_space_check_interval_ms", "ms"});
            put("dynamic_snitch_update_interval", new String[]{"dynamic_snitch_update_interval_in_ms", "ms"});
            put("dynamic_snitch_reset_interval", new String[]{"dynamic_snitch_reset_interval_in_ms", "ms"});
            put("gc_log_threshold", new String[]{"gc_log_threshold_in_ms", "ms"});
            put("hints_flush_period", new String[]{"hints_flush_period_in_ms", "ms"});
            put("gc_warn_threshold", new String[]{"gc_warn_threshold_in_ms", "ms"});
            put("tracetype_query_ttl", new String[]{"tracetype_query_ttl_in_s", "s"});
            put("tracetype_repair_ttl", new String[]{"tracetype_repair_ttl_in_s", "s"});
            put("permissions_validity", new String[]{"permissions_validity_in_ms", "ms"});
            put("permissions_update_interval", new String[]{"permissions_update_interval_in_ms", "ms"});
            put("roles_validity", new String[]{"roles_validity_in_ms", "ms"});
            put("roles_update_interval", new String[]{"roles_update_interval_in_ms", "ms"});
            put("credentials_validity", new String[]{"credentials_validity_in_ms", "ms"});
            put("credentials_update_interval", new String[]{"credentials_update_interval_in_ms", "ms"});
            put("index_summary_resize_interval", new String[]{"index_summary_resize_interval_in_minutes", "m"});
        }
    };

    private static final Map<String, String[]> MEM_UNITS_MAP = new HashMap<String, String[]>()
    {
        {
            put("max_hints_file_size", new String[]{ "max_hints_file_size_in_mb", "mb" });
            put("memtable_heap_space", new String[]{ "memtable_heap_space_in_mb", "mb" });
            put("memtable_offheap_space", new String[]{ "memtable_offheap_space_in_mb", "mb" });
            put("repair_session_space", new String[]{ "repair_session_space_in_mb", "mb" });
            put("internode_application_send_queue_capacity", new String[]{ "internode_application_send_queue_capacity_in_bytes", "b" });
            put("internode_application_send_queue_reserve_endpoint_capacity", new String[]{ "internode_application_send_queue_reserve_endpoint_capacity_in_bytes", "b" });
            put("internode_application_send_queue_reserve_global_capacity", new String[]{ "internode_application_send_queue_reserve_global_capacity_in_bytes", "b" });
            put("internode_application_receive_queue_capacity", new String[]{ "internode_application_receive_queue_capacity_in_bytes", "b" });
            put("internode_application_receive_queue_reserve_endpoint_capacity", new String[]{ "internode_application_receive_queue_reserve_endpoint_capacity_in_bytes", "b" });
            put("internode_application_receive_queue_reserve_global_capacity", new String[]{ "internode_application_receive_queue_reserve_global_capacity_in_bytes", "b" });
            put("max_native_transport_frame_size", new String[]{ "max_native_transport_frame_size_in_mb", "mb" });
            put("native_transport_frame_block_size", new String[]{ "native_transport_frame_block_size_in_kb", "kb" });
            put("max_value_size", new String[]{ "max_value_size_in_mb", "mb" });
            put("column_index_size", new String[]{ "column_index_size_in_kb", "kb" });
            put("column_index_cache_size", new String[]{ "column_index_cache_size_in_kb", "kb" });
            put("batch_size_warn_threshold", new String[]{ "batch_size_warn_threshold_in_kb", "kb" });
            put("batch_size_fail_threshold", new String[]{ "batch_size_fail_threshold_in_kb", "kb" });
            put("compaction_large_partition_warning_threshold", new String[]{ "compaction_large_partition_warning_threshold_mb", "mb" });
            put("commitlog_total_space", new String[]{ "commitlog_total_space_in_mb", "mb" });
            put("commitlog_segment_size", new String[]{ "commitlog_segment_size_in_mb", "mb" });
            put("max_mutation_size", new String[]{ "max_mutation_size_in_kb", "kb" });
            put("cdc_total_space", new String[]{ "cdc_total_space_in_mb", "mb" });
            put("hinted_handoff_throttle", new String[]{ "hinted_handoff_throttle_in_kb", "kb" });
            put("batchlog_replay_throttle", new String[]{ "batchlog_replay_throttle_in_kb", "kb" });
            put("trickle_fsync_interval", new String[]{ "trickle_fsync_interval_in_kb", "kb" });
            put("sstable_preemptive_open_interval", new String[]{ "sstable_preemptive_open_interval_in_mb", "mb" });
            put("counter_cache_size", new String[]{ "counter_cache_size_in_mb", "mb" });
            put("file_cache_size", new String[]{ "file_cache_size_in_mb", "mb" });
            put("index_summary_capacity", new String[]{ "index_summary_capacity_in_mb", "mb" });
            put("prepared_statements_cache_size", new String[]{ "prepared_statements_cache_size_mb", "mb" });
            put("key_cache_size", new String[]{ "key_cache_size_in_mb", "mb" });
            put("row_cache_size", new String[]{ "row_cache_size_in_mb", "mb" });
        }
    };

    private static final Map<String, String[]> RATE_UNITS_MAP = new HashMap<String, String[]>()
    {
        {
            put("compaction_throughput", new String[]{ "compaction_throughput_mb_per_sec", "mbps" });
            put("stream_throughput_outbound", new String[]{ "stream_throughput_outbound_megabits_per_sec", "mbps" });
            put("inter_dc_stream_throughput_outbound", new String[]{ "inter_dc_stream_throughput_outbound_megabits_per_sec", "mbps" });
        }
    };

    public static void parseDurationUnits(Config config, String contentStorageFile) throws NoSuchFieldException, IllegalAccessException
    {
        for (Map.Entry<String,String[]> entry : DURATION_UNITS_MAP.entrySet())
        {
            //grab the string field value
            String name = entry.getKey();
            Field stringField = Config.class.getField(name);
            String value;
            try
            {
                // Field.get() can throw NPE if the value of the field is null
                value = stringField.get(config).toString();
            }
            catch (NullPointerException | IllegalAccessException npe)
            {
                value = "null";
            }

            Field field = Config.class.getField(DURATION_UNITS_MAP.get(name)[0]);
            if (isBlank(entry.getKey(), contentStorageFile) &&
                (field.getGenericType().getTypeName().equals("long") || field.getGenericType().getTypeName().equals("int")
                 || field.getGenericType().getTypeName().equals("double")))
            {
                Field intField = Config.class.getField(entry.getValue()[0]);
                intField.set (config, null);
            }

            if(value.equals("null"))
                continue;

            if(value.equals("-1") && (name.equals("permissions_update_interval")
                                      || name.equals("roles_update_interval")
                                      || name.equals("credentials_update_interval")))
            {
                Config.class.getField(DURATION_UNITS_MAP.get(name)[0]).set(config, -1);
                continue;
            }

            if(name.equals("commitlog_sync_batch_window") || name.equals("commitlog_sync_group_window"))
            {
                parseTypeDouble(name, value, config, field);
                continue;
            }

            //parse the string field value
            Matcher matcher = TIME_UNITS_PATTERN.matcher(value);

            if (!matcher.find())
            {
                throw new ConfigurationException("Invalid yaml. This property " + name + '=' + value + " has invalid format." +
                                                 "Please check your units.", false);
            }

            TimeUnit sourceUnit = getCustomTimeUnit(matcher.group(2), name, value);

            setDurationParameterValue(name, value, field, config, sourceUnit, matcher);

        }
    }

    private static void setDurationParameterValue(String name, String value, Field field, Config config, TimeUnit sourceUnit, Matcher matcher) throws IllegalAccessException
    {
        switch(DURATION_UNITS_MAP.get(name)[1])
        {
            case "ms":
                switch(field.getGenericType().getTypeName())
                {
                    case "long":
                    case "java.lang.Long": field.set (config, sourceUnit.toMillis(Long.parseLong(matcher.group(1))));
                        break;
                    case "int":
                    case "java.lang.Integer": field.set (config, (int) sourceUnit.toMillis(Integer.parseInt(matcher.group(1))));
                        break;
                    default:
                        logger.info("field.getGenericType().getTypeName() {}", field.getGenericType().getTypeName());
                        throw new ConfigurationException("Not handled parameter type.");
                }
                break;
            case "s":
                switch(field.getGenericType().getTypeName())
                {
                    case "long":
                    case "java.lang.Long": field.set (config, sourceUnit.toSeconds(Long.parseLong(matcher.group(1))));
                        break;
                    case "int":
                    case "java.lang.Integer":
                        field.set (config, Math.toIntExact(sourceUnit.toSeconds( Integer.parseInt(matcher.group(1)))));
                        break;
                    default:
                        logger.info("field.getGenericType().getTypeName() {}", field.getGenericType().getTypeName());
                        throw new ConfigurationException("Not handled parameter type.");
                }
                break;
            case "m":
                switch(field.getGenericType().getTypeName())
                {
                    case "long":
                    case "java.lang.Long": field.set (config, sourceUnit.toMinutes(Long.parseLong(matcher.group(1))));
                        break;
                    case "int":
                    case "java.lang.Integer":
                        field.set (config, Math.toIntExact(sourceUnit.toMinutes(Integer.parseInt(matcher.group(1)))));
                        break;
                    default:
                        logger.info("field.getGenericType().getTypeName() {}", field.getGenericType().getTypeName());
                        throw new ConfigurationException("Not handled parameter type.");
                }
                break;
            default:
                logger.info("field.getGenericType().getTypeName() {}", field.getGenericType().getTypeName());
                throw new ConfigurationException("Invalid yaml. This property " + name + '=' + value + " has invalid format." +
                                                 "Please check your units.", false);
        }
    }

    private static void parseTypeDouble(String name, String value, Config config, Field field) throws IllegalAccessException
    {
        //parse the string field value
        Matcher matcherDouble = DOUBLE_TIME_UNITS_PATTERN.matcher(value);

        if (!matcherDouble.find())
        {
            matcherDouble = TIME_UNITS_PATTERN.matcher(value);

            if (!matcherDouble.find())
            {
                throw new ConfigurationException("Invalid yaml. This property " + name + '=' + value + " has invalid format." +
                                                 "Please check your units.", false);
            }
        }

        DoubleTimeUnit sourceUnitDouble = getCustomTimeUnitDouble(matcherDouble.group(2), name, value);

        setDoubleDurationParameterValue(name, field, config, sourceUnitDouble, matcherDouble);
    }

    private static void setDoubleDurationParameterValue(String name, Field field, Config config, DoubleTimeUnit sourceUnitDouble, Matcher matcherDouble) throws IllegalAccessException
    {
        switch(DURATION_UNITS_MAP.get(name)[1])
        {
            case "ms":
                field.set(config, sourceUnitDouble.toMillis(Double.parseDouble(matcherDouble.group(1))));
                break;
            case "s":
                field.set(config, sourceUnitDouble.toSeconds(Double.parseDouble(matcherDouble.group(1))));
                break;
            case "m":
                field.set(config, sourceUnitDouble.toMinutes(Double.parseDouble(matcherDouble.group(1))));
            default:
                logger.info("field.getGenericType().getTypeName() {}", field.getGenericType().getTypeName());
                throw new ConfigurationException("Not handled parameter type.");
        }
    }

    public static void parseMemUnits(Config config, String contentStorageFile) throws NoSuchFieldException, IllegalAccessException
    {

        for (Map.Entry<String,String[]> entry : MEM_UNITS_MAP.entrySet())
        {
            //grab the string field value
            String name = entry.getKey();
            Field stringField = Config.class.getField(name);
            String value;
            try
            {
                // Field.get() can throw NPE if the value of the field is null
                value = stringField.get(config).toString();
            }
            catch (NullPointerException | IllegalAccessException npe)
            {
                value = "null";
            }

            Field field = Config.class.getField(MEM_UNITS_MAP.get(name)[0]);
            if (isBlank(entry.getKey(), contentStorageFile) &&
                (field.getGenericType().getTypeName().equals("long") || field.getGenericType().getTypeName().equals("int")
                 || field.getGenericType().getTypeName().equals("double")))
            {
                Field intField = Config.class.getField(entry.getValue()[0]);
                intField.set (config, null);
            }

            if(value.equals("null"))
                continue;

            //parse the string field value
            Matcher matcher = MEMORY_UNITS_PATTERN.matcher(value);

            if (!matcher.find())
            {
                throw new ConfigurationException("Invalid yaml. This property " + name + '=' + value + " has invalid format." +
                                                 "Please check your units.", false);
            }

            MemUnit sourceUnit = getCustomMemUnit(matcher.group(2), name, value);

            setMemoryParameterValue(name, value, field, config, sourceUnit, matcher);

        }
    }

    private static void setMemoryParameterValue(String name, String value, Field field, Config config, MemUnit sourceUnit, Matcher matcher) throws IllegalAccessException
    {
        switch(MEM_UNITS_MAP.get(name)[1])
        {
            case "b":
                switch(field.getGenericType().getTypeName())
                {
                    case "long":
                    case "java.lang.Long":
                        field.set (config, sourceUnit.toBytes(Long.parseLong(matcher.group(1))));
                        break;
                    case "int":
                    case "java.lang.Integer":
                        field.set (config, Math.toIntExact(sourceUnit.toBytes(Integer.parseInt(matcher.group(1)))));
                        break;
                    default:
                        throw new ConfigurationException("Not handled parameter type.");
                }
                break;
            case "kb":
                switch(field.getGenericType().getTypeName())
                {
                    case "long":
                    case "Long":
                        field.set (config, sourceUnit.toKB(Long.parseLong(matcher.group(1))));
                        break;
                    case "int":
                    case "java.lang.Integer":
                        field.set (config, (int)sourceUnit.toKB(Integer.parseInt(matcher.group(1))));
                        break;
                    default:
                        logger.info("field.getGenericType().getTypeName() {}", field.getGenericType().getTypeName());
                        throw new ConfigurationException("Not handled parameter type.");
                }
                break;
            case "mb":
                switch(field.getGenericType().getTypeName())
                {
                    case "long":
                    case "java.lang.Long":
                        field.set (config, sourceUnit.toMB(Long.parseLong(matcher.group(1))));
                        break;
                    case "int":
                    case "java.lang.Integer": field.set (config, Math.toIntExact(sourceUnit.toMB(Integer.parseInt(matcher.group(1)))));
                        break;
                    default:
                        logger.info("field.getGenericType().getTypeName() {}", field.getGenericType().getTypeName());
                        throw new ConfigurationException("Not handled parameter type.");
                }
                break;
            default:
                throw new ConfigurationException("Invalid yaml. This property " + name + '=' + value + " has invalid format." +
                                                 "Please check your units.", false);
        }
    }

    public static void parseRateUnits(Config config, String contentStorageFile) throws NoSuchFieldException, IllegalAccessException
    {

        for (Map.Entry<String,String[]> entry : RATE_UNITS_MAP.entrySet())
        {
            //grab the string field value
            String name = entry.getKey();
            Field stringField = Config.class.getField(name);
            String value;
            try
            {
                // Field.get() can throw NPE if the value of the field is null
                value = stringField.get(config).toString();
            }
            catch (NullPointerException | IllegalAccessException npe)
            {
                value = "null";
            }

            Field field = Config.class.getField(RATE_UNITS_MAP.get(name)[0]);
            if (isBlank(entry.getKey(), contentStorageFile) &&
                (field.getGenericType().getTypeName().equals("long") || field.getGenericType().getTypeName().equals("int")
                 || field.getGenericType().getTypeName().equals("double")))
            {
                Field intField = Config.class.getField(entry.getValue()[0]);
                intField.set (config, null);
            }

            if(value.equals("null"))
                continue;

            //parse the string field value
            Matcher matcher = RATE_UNITS_PATTERN.matcher(value);

            if (!matcher.find())
            {
                throw new ConfigurationException("Invalid yaml. This property " + name + '=' + value + " has invalid format." +
                                                 "Please check your units.", false);
            }

            RateUnit sourceUnit = getCustomRateUnit(matcher.group(2), name, value);

            setRateParameterValue(name, value, field, config, sourceUnit, matcher);
        }
    }

    private static void setRateParameterValue(String name, String value, Field field, Config config, RateUnit sourceUnit, Matcher matcher) throws IllegalAccessException
    {
        switch(RATE_UNITS_MAP.get(name)[1])
        {
            case "bps":  field.set (config, Math.toIntExact(sourceUnit.toBps(Integer.parseInt(matcher.group(1))))); break;
            case "kbps":
            case "Kbps": field.set (config, Math.toIntExact(sourceUnit.toKbps(Integer.parseInt(matcher.group(1)))));break;
            case "mbps":
            case "Mbps": field.set (config, Math.toIntExact(sourceUnit.toMbps(Integer.parseInt(matcher.group(1)))));break;
            default: throw new ConfigurationException("Invalid yaml. This property " + name + '=' + value + " has invalid format." +
                                                      "Please check your units.", false);
        }
    }

    private static TimeUnit getCustomTimeUnit(String unit, String fieldName, String fieldValue)
    {
        TimeUnit sourceUnit;

        switch (unit.toLowerCase())
        {
            case "ns": sourceUnit = TimeUnit.NANOSECONDS; break;
            case "s":  sourceUnit = TimeUnit.SECONDS;     break;
            case "m":  sourceUnit = TimeUnit.MINUTES;     break;
            case "h":  sourceUnit = TimeUnit.HOURS;       break;
            case "d":  sourceUnit = TimeUnit.DAYS;        break;
            case "µs":
            case "us": sourceUnit = TimeUnit.MICROSECONDS; break;
            case "ms": sourceUnit = TimeUnit.MILLISECONDS; break;
            default:
                throw new IllegalStateException("Unexpected unit " + fieldName + ':' + fieldValue );
        }

        return sourceUnit;
    }

    private static DoubleTimeUnit getCustomTimeUnitDouble(String unit, String fieldName, String fieldValue)
    {
        DoubleTimeUnit sourceUnit;

        switch (unit.toLowerCase())
        {
            case "s":  sourceUnit = DoubleTimeUnit.SECONDS;     break;
            case "m":  sourceUnit = DoubleTimeUnit.MINUTES;     break;
            case "ms": sourceUnit = DoubleTimeUnit.MILLISECONDS; break;
            default:
                throw new IllegalStateException("Unexpected unit " + fieldName + ':' + fieldValue );
        }

        return sourceUnit;
    }

    private static MemUnit getCustomMemUnit(String unit, String fieldName, String fieldValue)
    {
        MemUnit sourceUnit;

        switch (unit.toLowerCase())
        {
            case "b":  sourceUnit = MemUnit.BYTES;     break;
            case "kb": sourceUnit = MemUnit.KILOBYTES; break;
            case "mb": sourceUnit = MemUnit.MEGABYTES; break;
            default: throw new IllegalStateException("Unexpected unit " + fieldName +':' + fieldValue );
        }

        return sourceUnit;

    }

    private static RateUnit getCustomRateUnit(String unit, String fieldName, String fieldValue)
    {
        RateUnit sourceUnit;

        switch (unit)
        {
            case "bps":  sourceUnit = RateUnit.BITSPERSECOND; break;
            case "kbps":
            case "Kbps": sourceUnit = RateUnit.KILOBITSPERSECOND;    break;
            case "mbps":
            case "Mbps": sourceUnit = RateUnit.MEGABITSPERSECOND;    break;
            default:  throw new IllegalStateException("Unexpected unit " + fieldName +':' + fieldValue );
        }

        return sourceUnit;
    }

    private enum MemUnit {
        BYTES
        {
            public long toBytes(long d) { return d; }
            public long toKB(long d)    { return d / 1024; }
            public long toMB(long d)    { return d / (1024 * 1024); }
        },
        KILOBYTES
        {
            public long toBytes(long d) { return x(d, 1024, MAX/1024); }
            public long toKB(long d)    { return d; }
            public long toMB(long d)    { return d / 1024; }
        },
        MEGABYTES
        {
            public long toBytes(long d) { return x(d, 1024 * 1024, MAX/(1024 * 1024)); }
            public long toKB(long d)    { return x(d, 1024, MAX/1024); }
            public long toMB(long d)    { return d; }
        };

        /**
         * Scale d by m, checking for overflow.
         * This has a short name to make above code more readable.
         */
        static long x(long d, long m, long over) {
            if (d >  over) return Long.MAX_VALUE;
            return d * m;
        }

        static final long MAX = Long.MAX_VALUE;

        public long toBytes(long d) {
            throw new AbstractMethodError();
        }
        public long toKB(long d) {
            throw new AbstractMethodError();
        }
        public long toMB(long d) {
            throw new AbstractMethodError();
        }
    }

    private enum RateUnit {
        BITSPERSECOND
        {
            public long toBps(long d) { return d; }
            public long toKbps(long d) { return d / 1000; }
            public long toMbps(long d) { return d / (1000 * 1000) ; }
        },
        KILOBITSPERSECOND
        {
            public long toBps(long d) { return x(d, 1000, MAX/1000); }
            public long toKbps(long d) { return d; }
            public long toMbps(long d) { return d / 1000; }
        },
        MEGABITSPERSECOND
        {
            public long toBps(long d) { return x(d, 1000 * 1000, MAX/(1000 * 1000)); }
            public long toKbps(long d) { return x(d, 1000, MAX/(1000)); }
            public long toMbps(long d) { return d; }
        };

        /**
         * Scale d by m, checking for overflow.
         * This has a short name to make above code more readable.
         */
        static long x(long d, long m, long over) {
            if (d >  over) return Long.MAX_VALUE;
            return d * m;
        }
        static final long MAX = Long.MAX_VALUE;

        public long toBps(long d) {
            throw new AbstractMethodError();
        }
        public long toKbps(long d) {
            throw new AbstractMethodError();
        }
        public long toMbps(long d) {
            throw new AbstractMethodError();
        }
    }

    private enum DoubleTimeUnit
    {
        MILLISECONDS {
            public double toMillis(double d)  { return d; }
            public double toSeconds(double d) { return d/(C3/C2); }
            public double toMinutes(double d) { return d/(C4/C2); }
            public double convert(double d, ParseAndConvertUnits.DoubleTimeUnit u) { return u.toMillis(d); }
            double excessNanos(double d, double m) { return 0; }
        },
        SECONDS {
            public double toMillis(double d)  { return x(d, C3/C2, MAX/(C3/C2)); }
            public double toSeconds(double d) { return d; }
            public double toMinutes(double d) { return d/(C4/C3); }
            public double convert(double d, ParseAndConvertUnits.DoubleTimeUnit u) { return u.toSeconds(d); }
            double excessNanos(double d, double m) { return 0; }
        },
        MINUTES {
            public double toMillis(double d)  { return x(d, C4/C2, MAX/(C4/C2)); }
            public double toSeconds(double d) { return x(d, C4/C3, MAX/(C4/C3)); }
            public double toMinutes(double d) { return d; }
            public double convert(double d, ParseAndConvertUnits.DoubleTimeUnit u) { return u.toMinutes(d); }
            double excessNanos(double d, double m) { return 0; }
        };

        // Handy constants for conversion methods
        static final double C0 = 1L;
        static final double C1 = C0 * 1000L;
        static final double C2 = C1 * 1000L;
        static final double C3 = C2 * 1000L;
        static final double C4 = C3 * 60L;

        static final double MAX = Double.MAX_VALUE;

        /**
         * Scale d by m, checking for overflow.
         * This has a short name to make above code more readable.
         */
        static double x(double d, double m, double over)
        {
            if (d > over) return Double.MAX_VALUE;
            if (d < -over) return Double.MIN_VALUE;
            return d * m;
        }

        public double convert(double sourceDuration, DoubleTimeUnit sourceUnit) {
            throw new AbstractMethodError();
        }

        /**
         * Equivalent to <tt>MILLISECONDS.convert(duration, this)</tt>.
         * @param duration the duration
         * @return the converted duration,
         * or <tt>Long.MIN_VALUE</tt> if conversion would negatively
         * overflow, or <tt>Long.MAX_VALUE</tt> if it would positively overflow.
         * @see #convert
         */
        public double toMillis(double duration) {
            throw new AbstractMethodError();
        }

        /**
         * Equivalent to <tt>SECONDS.convert(duration, this)</tt>.
         * @param duration the duration
         * @return the converted duration,
         * or <tt>Long.MIN_VALUE</tt> if conversion would negatively
         * overflow, or <tt>Long.MAX_VALUE</tt> if it would positively overflow.
         * @see #convert
         */
        public double toSeconds(double duration) {
            throw new AbstractMethodError();
        }

        /**
         * Equivalent to <tt>MINUTES.convert(duration, this)</tt>.
         * @param duration the duration
         * @return the converted duration,
         * or <tt>Long.MIN_VALUE</tt> if conversion would negatively
         * overflow, or <tt>Long.MAX_VALUE</tt> if it would positively overflow.
         * @see #convert
         * @since 1.6
         */
        public double toMinutes(double duration) {
            throw new AbstractMethodError();
        }
    }

    private static boolean isBlank(String property, String contentStorageFile)
    {
        Pattern p = Pattern.compile(String.format("%s%s *: *$", '^', property), Pattern.MULTILINE);
        Matcher m = p.matcher(contentStorageFile);
        return m.find();
    }
}

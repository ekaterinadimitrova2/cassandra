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

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LoadOldYAMLBackwardCompatibilityTest
{
    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        System.setProperty("cassandra.config", "cassandra-old.yaml");
        DatabaseDescriptor.daemonInitialization();
    }

    // CASSANDRA-15234
    @Test
    public void testConfigurationLoaderBackwardCompatibility()
    {
        Config config = DatabaseDescriptor.loadConfig();

        assertEquals(new SmallestDuration.IntMilliseconds(10800000), config.max_hint_window);
        assertEquals(new SmallestDuration.IntMilliseconds("3h"), config.max_hint_window);
        assertEquals(new SmallestDuration.Milliseconds(0), config.native_transport_idle_timeout);
        assertEquals(new SmallestDuration.Milliseconds(10000), config.request_timeout);
        assertEquals(new SmallestDuration.Milliseconds(5000), config.read_request_timeout);
        assertEquals(new SmallestDuration.Milliseconds(10000), config.range_request_timeout);
        assertEquals(new SmallestDuration.Milliseconds(2000), config.write_request_timeout);
        assertEquals(new SmallestDuration.Milliseconds(5000), config.counter_write_request_timeout);
        assertEquals(new SmallestDuration.Milliseconds(1800), config.cas_contention_timeout);
        assertEquals(new SmallestDuration.Milliseconds(60000), config.truncate_request_timeout);
        assertEquals(new SmallestDuration.IntSeconds(300), config.streaming_keep_alive_period);
        assertEquals(new SmallestDuration.Milliseconds(500), config.slow_query_log_timeout);
        assertNull(config.memtable_heap_space);
        assertNull(config.memtable_offheap_space);
        assertNull( config.repair_session_space);
        assertEquals(new SmallestDataStorage.IntBytes(4194304), config.internode_application_send_queue_capacity);
        assertEquals(new SmallestDataStorage.IntBytes(134217728), config.internode_application_send_queue_reserve_endpoint_capacity);
        assertEquals(new SmallestDataStorage.IntBytes(536870912), config.internode_application_send_queue_reserve_global_capacity);
        assertEquals(new SmallestDataStorage.IntBytes(4194304), config.internode_application_receive_queue_capacity);
        assertEquals(new SmallestDataStorage.IntBytes(134217728), config.internode_application_receive_queue_reserve_endpoint_capacity);
        assertEquals(new SmallestDataStorage.IntBytes(536870912), config.internode_application_receive_queue_reserve_global_capacity);
        assertEquals(new SmallestDuration.IntMilliseconds(2000), config.internode_tcp_connect_timeout);
        assertEquals(new SmallestDuration.IntMilliseconds(30000), config.internode_tcp_user_timeout);
        assertEquals(new SmallestDuration.IntMilliseconds(300000), config.internode_streaming_tcp_user_timeout);
        assertEquals(new SmallestDataStorage.IntMebibytes(16), config.native_transport_max_frame_size);
        assertEquals(new SmallestDataStorage.IntMebibytes(256), config.max_value_size);
        assertEquals(new SmallestDataStorage.IntKibibytes(4), config.column_index_size);
        assertEquals(new SmallestDataStorage.IntKibibytes(2), config.column_index_cache_size);
        assertEquals(new SmallestDataStorage.IntKibibytes(5), config.batch_size_warn_threshold);
        assertEquals(IntDataRate.inMebibytesPerSecond(64), config.compaction_throughput);
        assertEquals(new SmallestDataStorage.IntMebibytes(50), config.min_free_space_per_drive);
        assertEquals(IntDataRate.inMebibytesPerSecond(23841858).toString(), config.stream_throughput_outbound.toString());
        assertEquals(IntDataRate.megabitsPerSecondInMebibytesPerSecond(200000000).toString(), config.stream_throughput_outbound.toString());
        assertEquals(IntDataRate.inMebibytesPerSecond(24), config.inter_dc_stream_throughput_outbound);
        assertNull(config.commitlog_total_space);
        assertEquals(new SmallestDuration.IntMilliseconds(0.0, TimeUnit.MILLISECONDS), config.commitlog_sync_group_window);
        assertEquals(new SmallestDuration.IntMilliseconds(0), config.commitlog_sync_period);
        assertEquals(new SmallestDataStorage.IntMebibytes(5), config.commitlog_segment_size);
        assertNull(config.periodic_commitlog_sync_lag_block);  //Integer
        assertNull(config.max_mutation_size);
        assertEquals(new SmallestDataStorage.IntMebibytes(0), config.cdc_total_space);
        assertEquals(new SmallestDuration.IntMilliseconds(250), config.cdc_free_space_check_interval);
        assertEquals(new SmallestDuration.IntMilliseconds(100), config.dynamic_snitch_update_interval);
        assertEquals(new SmallestDuration.IntMilliseconds(600000), config.dynamic_snitch_reset_interval);
        assertEquals(new SmallestDataStorage.IntKibibytes(1024), config.hinted_handoff_throttle);
        assertEquals(new SmallestDataStorage.IntKibibytes(1024), config.batchlog_replay_throttle);
        assertEquals(new SmallestDuration.IntMilliseconds(10000), config.hints_flush_period);
        assertEquals(new SmallestDataStorage.IntMebibytes(128), config.max_hints_file_size);
        assertEquals(new SmallestDataStorage.IntKibibytes(10240), config.trickle_fsync_interval);
        assertEquals(new SmallestDataStorage.IntMebibytes(50), config.sstable_preemptive_open_interval);
        assertNull( config.key_cache_size);
        assertEquals(new SmallestDataStorage.Mebibytes(16), config.row_cache_size);
        assertNull(config.counter_cache_size);
        assertNull(config.networking_cache_size);
        assertNull(config.file_cache_size);
        assertNull(config.index_summary_capacity);
        assertEquals(new SmallestDuration.IntMilliseconds(200), config.gc_log_threshold);
        assertEquals(new SmallestDuration.IntMilliseconds(1000), config.gc_warn_threshold);
        assertEquals(new SmallestDuration.IntSeconds(86400), config.trace_type_query_ttl);
        assertEquals(new SmallestDuration.IntSeconds(604800), config.trace_type_repair_ttl);
        assertNull(config.prepared_statements_cache_size);
        assertTrue(config.user_defined_functions_enabled);
        assertTrue(config.scripted_user_defined_functions_enabled);
        assertTrue(config.materialized_views_enabled);
        assertFalse(config.transient_replication_enabled);
        assertTrue(config.sasi_indexes_enabled);
        assertTrue(config.drop_compact_storage_enabled);
        assertTrue(config.user_defined_functions_threads_enabled);
        assertEquals(new SmallestDuration.IntMilliseconds(2000), config.permissions_validity);
        assertNull(config.permissions_update_interval);
        assertEquals(new SmallestDuration.IntMilliseconds(2000), config.roles_validity);
        assertNull(config.roles_update_interval);
        assertEquals(new SmallestDuration.IntMilliseconds(2000), config.credentials_validity);
        assertNull(config.credentials_update_interval);
        assertEquals(new SmallestDuration.IntMinutes(60), config.index_summary_resize_interval);

        //parameters which names have not changed with CASSANDRA-15234
        assertEquals(SmallestDuration.IntSeconds.inSecondsString("14400"), config.key_cache_save_period);
        assertEquals(SmallestDuration.IntSeconds.inSecondsString("14400s"), config.key_cache_save_period);
        assertEquals(new SmallestDuration.IntSeconds(4, TimeUnit.HOURS), config.key_cache_save_period);
        assertEquals(SmallestDuration.IntSeconds.inSecondsString("0"), config.row_cache_save_period);
        assertEquals(new SmallestDuration.IntSeconds(0), config.row_cache_save_period);
        assertEquals(new SmallestDuration.IntSeconds(2, TimeUnit.HOURS), config.counter_cache_save_period);
        assertEquals(new SmallestDuration.IntSeconds(35), config.cache_load_timeout);
    }
}

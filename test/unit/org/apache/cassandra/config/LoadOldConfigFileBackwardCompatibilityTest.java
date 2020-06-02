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

import java.net.URL;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.junit.Assert.assertEquals;

@RunWith(OrderedJUnit4ClassRunner.class)
public class LoadOldConfigFileBackwardCompatibilityTest
{
    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        System.setProperty("cassandra.config", "cassandra-old.yaml");
        DatabaseDescriptor.daemonInitialization();
    }

    // CASSANDRA-15234
    @Test
    public void testConfigurationLoaderBackwardCompatibility() throws Exception
    {

        Config config = DatabaseDescriptor.loadConfig();

        //Confirm parameters were successfully read with the default values in cassandra-old.yaml
        assertEquals(10800000, config.max_hint_window_in_ms);
        assertEquals(0, config.native_transport_idle_timeout_in_ms);
        assertEquals(10000, config.request_timeout_in_ms);
        assertEquals(5000, config.read_request_timeout_in_ms);
        assertEquals(10000, config.range_request_timeout_in_ms);
        assertEquals(2000, config.write_request_timeout_in_ms);
        assertEquals(5000, config.counter_write_request_timeout_in_ms);
        assertEquals(1000, config.cas_contention_timeout_in_ms);
        assertEquals(60000, config.truncate_request_timeout_in_ms);
        assertEquals(300, config.streaming_keep_alive_period_in_secs, 0);
        assertEquals(500, config.slow_query_log_timeout_in_ms);
        assertEquals(2000, config.internode_tcp_connect_timeout_in_ms);
        assertEquals(30000, config.internode_tcp_user_timeout_in_ms);
        assertEquals(1.0, config.commitlog_sync_batch_window_in_ms, 0);
        assertEquals(Double.NaN, config.commitlog_sync_group_window_in_ms,0); //double
        assertEquals(0, config.commitlog_sync_period_in_ms);
        assertEquals(null, config.periodic_commitlog_sync_lag_block_in_ms);  //Integer
        assertEquals(250, config.cdc_free_space_check_interval_ms);
        assertEquals(100, config.dynamic_snitch_update_interval_in_ms);
        assertEquals(600000, config.dynamic_snitch_reset_interval_in_ms);
        assertEquals(200, config.gc_log_threshold_in_ms);
        assertEquals(10000, config.hints_flush_period_in_ms);
        assertEquals(1000, config.gc_warn_threshold_in_ms);
        assertEquals(86400, config.tracetype_query_ttl_in_s);
        assertEquals(604800, config.tracetype_repair_ttl_in_s);
        assertEquals(2000, config.permissions_validity_in_ms);
        assertEquals(-1, config.permissions_update_interval_in_ms);
        assertEquals(2000, config.roles_validity_in_ms);
        assertEquals(-1, config.roles_update_interval_in_ms);
        assertEquals(2000, config.credentials_validity_in_ms);
        assertEquals(-1, config.credentials_update_interval_in_ms);
        assertEquals(60, config.index_summary_resize_interval_in_minutes);
    }
};





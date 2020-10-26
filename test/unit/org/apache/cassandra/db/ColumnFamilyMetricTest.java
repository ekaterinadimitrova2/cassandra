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
package org.apache.cassandra.db;

import java.util.Collection;
import java.util.EventListener;

import javax.management.ObjectName;

import com.codahale.metrics.SharedMetricRegistries;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.Timer;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;

import static org.junit.Assert.assertEquals;

public class ColumnFamilyMetricTest
{
    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("Keyspace1",
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD("Keyspace1", "Standard2"));
    }

    @Test
    public void testSizeMetric()
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard2");
        cfs.disableAutoCompaction();

        cfs.truncateBlocking();

        assertEquals(0, cfs.metric.liveDiskSpaceUsed.getCount());
        assertEquals(0, cfs.metric.totalDiskSpaceUsed.getCount());

        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(cfs.metadata(), FBUtilities.timestampMicros(), String.valueOf(j))
                    .clustering("0")
                    .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();
        }
        cfs.forceBlockingFlush();
        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        long size = 0;
        for (SSTableReader reader : sstables)
        {
            size += reader.bytesOnDisk();
        }

        // size metrics should show the sum of all SSTable sizes
        assertEquals(size, cfs.metric.liveDiskSpaceUsed.getCount());
        assertEquals(size, cfs.metric.totalDiskSpaceUsed.getCount());

        cfs.truncateBlocking();

        // after truncate, size metrics should be down to 0
        Util.spinAssertEquals(0L, () -> cfs.metric.liveDiskSpaceUsed.getCount(), 30);
        Util.spinAssertEquals(0L, () -> cfs.metric.totalDiskSpaceUsed.getCount(), 30);

        cfs.enableAutoCompaction();
    }

    @Test
    public void testColUpdateTimeDeltaFiltering()
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard2");

        // This confirms another test/set up did not overflow the histogram
        store.metric.colUpdateTimeDeltaHistogram.cf.getSnapshot().get999thPercentile();

        new RowUpdateBuilder(store.metadata(), 0, "4242")
            .clustering("0")
            .add("val", ByteBufferUtil.bytes("0"))
            .build()
            .applyUnsafe();

        // The histogram should not have overflowed on the first write
        store.metric.colUpdateTimeDeltaHistogram.cf.getSnapshot().get999thPercentile();

        // smallest time delta that would overflow the histogram if unfiltered
        new RowUpdateBuilder(store.metadata(), 18165375903307L, "4242")
            .clustering("0")
            .add("val", ByteBufferUtil.bytes("0"))
            .build()
            .applyUnsafe();

        // CASSANDRA-11117 - update with large timestamp delta should not overflow the histogram
        store.metric.colUpdateTimeDeltaHistogram.cf.getSnapshot().get999thPercentile();
    }

    @Test
    public void testStartupRaceConditionOnMetricListeners()
    {
        // Since the ColumnFamilyStore instance reference escapes during the construction
        // we have a race condition and listeners can see an instance that is in an unknown state
        // this test just check that all callbacks can access the data without throwing any exception.
        registerMetricListener();
        SchemaLoader.createKeyspace("Keyspace2",
                KeyspaceParams.simple(1),
                SchemaLoader.standardCFMD("Keyspace2", "Standard2"));
    }

    private void registerMetricListener()
    {
        CassandraMetricsRegistry.Metrics.addListener(new MetricRegistryListener.Base()
        {
            @Override
            public void onGaugeAdded(String name, Gauge<?> gauge)
            {
                gauge.getValue();
            }

            @Override
            public void onGaugeRemoved(String name)
            {

            }

            @Override
            public void onCounterAdded(String name, Counter counter)
            {
                counter.getCount();
            }

            @Override
            public void onCounterRemoved(String name)
            {

            }

            @Override
            public void onHistogramAdded(String name, Histogram histogram)
            {
                histogram.getCount();
            }

            @Override
            public void onHistogramRemoved(String name)
            {

            }

            @Override
            public void onMeterAdded(String name, Meter meter)
            {
                meter.getCount();
            }

            @Override
            public void onMeterRemoved(String name)
            {

            }

            @Override
            public void onTimerAdded(String name, Timer timer)
            {
                timer.getCount();
            }

            @Override
            public void onTimerRemoved(String name)
            {

            }
        });

        SharedMetricRegistries.getOrCreate("logback-metrics").addListener(new MetricRegistryListener.Base()
        {
            @Override
            public void onMeterAdded(String metricName, Meter meter)
            {
                // Given metricName consists of appender name in logback.xml + "." + metric name.
                // We first separate appender name
                int separator = metricName.lastIndexOf('.');
                String appenderName = metricName.substring(0, separator);
                String metric = metricName.substring(separator + 1); // remove "."
                ObjectName name = DefaultNameFactory.createMetricName(appenderName, metric, null).getMBeanName();
                CassandraMetricsRegistry.Metrics.registerMBean(meter, name);
            }
        });
    }
}

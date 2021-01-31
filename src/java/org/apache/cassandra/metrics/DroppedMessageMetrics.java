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
package org.apache.cassandra.metrics;

import com.google.common.collect.ImmutableMap;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for dropped messages by verb.
 */
public class DroppedMessageMetrics
{
    /** Number of dropped messages */
    public final Meter dropped;

    /** The dropped latency within node */
    public final Timer internalDroppedLatency;

    /** The cross node dropped latency */
    public final Timer crossNodeDroppedLatency;

    public DroppedMessageMetrics(Verb verb)
    {
        this(new DefaultNameFactory("DroppedMessage", verb.toString()));
    }

    public DroppedMessageMetrics(DefaultNameFactory factory)
    {
        String currentScope = factory.getScope();
        // backward compatibility for request metrics which names have changed in 4.0 as part of CASSANDRA-15066
        ImmutableMap<String, String> requestVerbsAlias = ImmutableMap.<String, String>builder()
                                                         .put("BATCH_REMOVE_REQ", "BATCH_REMOVE")
                                                         .put("BATCH_STORE_REQ", "BATCH_STORE")
                                                         .put("COUNTER_MUTATION_REQ", "COUNTER_MUTATION")
                                                         .put("HINT_REQ", "HINT")
                                                         .put("MUTATION_REQ", "MUTATION")
                                                         .put("RANGE_REQ", "RANGE_SLICE")
                                                         .put("READ_REQ", "READ")
                                                         .put("READ_REPAIR_REQ", "READ_REPAIR")
                                                         .put("REQUEST_RSP", "REQUEST_RESPONSE")
                                                         .build();

        if (requestVerbsAlias.containsKey(currentScope))
        {
            dropped = Metrics.meter(factory.createMetricName("Dropped"),
                                    DefaultNameFactory.createMetricName("DroppedMessage", "Dropped", requestVerbsAlias.get(currentScope)));
            internalDroppedLatency = Metrics.timer(factory.createMetricName("InternalDroppedLatency"),
                                                   DefaultNameFactory.createMetricName("DroppedMessage", "InternalDroppedLatency", requestVerbsAlias.get(currentScope)));
            crossNodeDroppedLatency = Metrics.timer(factory.createMetricName("CrossNodeDroppedLatency"),
                                                    DefaultNameFactory.createMetricName("DroppedMessage", "CrossNodeDroppedLatency", requestVerbsAlias.get(currentScope)));
        }
        else
        {
            dropped = Metrics.meter(factory.createMetricName("Dropped"));
            internalDroppedLatency = Metrics.timer(factory.createMetricName("InternalDroppedLatency"));
            crossNodeDroppedLatency = Metrics.timer(factory.createMetricName("CrossNodeDroppedLatency"));
        }
    }

    public DroppedMessageMetrics(MetricNameFactory factory)
    {
        dropped = Metrics.meter(factory.createMetricName("Dropped"));
        internalDroppedLatency = Metrics.timer(factory.createMetricName("InternalDroppedLatency"));
        crossNodeDroppedLatency = Metrics.timer(factory.createMetricName("CrossNodeDroppedLatency"));
    }
}

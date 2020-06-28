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

package org.apache.cassandra.distributed.impl;

import java.io.File;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DataStorage;
import org.apache.cassandra.config.Duration;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SimpleSeedProvider;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.composer.Composer;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.Mark;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.SequenceNode;
import org.yaml.snakeyaml.nodes.Tag;

import static org.apache.cassandra.config.YamlConfigurationLoader.getReplacements;

public class InstanceConfig implements IInstanceConfig
{
    private static final Object NULL = new Object();
    private static final Logger logger = LoggerFactory.getLogger(InstanceConfig.class);

    public final int num;
    public int num() { return num; }

    private final NetworkTopology networkTopology;
    public NetworkTopology networkTopology() { return networkTopology; }

    public final UUID hostId;
    public UUID hostId() { return hostId; }
    private final Map<String, Object> params = new TreeMap<>();

    private final EnumSet featureFlags;

    private volatile InetAddressAndPort broadcastAddressAndPort;

    private InstanceConfig(int num,
                           NetworkTopology networkTopology,
                           String broadcast_address,
                           String listen_address,
                           String broadcast_rpc_address,
                           String rpc_address,
                           String seedIp,
                           int seedPort,
                           String saved_caches_directory,
                           String[] data_file_directories,
                           String commitlog_directory,
                           String hints_directory,
                           String cdc_raw_directory,
                           String initial_token,
                           int storage_port,
                           int native_transport_port)
    {
        this.num = num;
        this.networkTopology = networkTopology;
        this.hostId = java.util.UUID.randomUUID();
        this    .set("num_tokens", 1)
                .set("broadcast_address", broadcast_address)
                .set("listen_address", listen_address)
                .set("broadcast_rpc_address", broadcast_rpc_address)
                .set("rpc_address", rpc_address)
                .set("saved_caches_directory", saved_caches_directory)
                .set("data_file_directories", data_file_directories)
                .set("commitlog_directory", commitlog_directory)
                .set("hints_directory", hints_directory)
                .set("cdc_raw_directory", cdc_raw_directory)
                .set("initial_token", initial_token)
                .set("partitioner", "org.apache.cassandra.dht.Murmur3Partitioner")
                .set("start_native_transport", true)
                .set("concurrent_writes", 2)
                .set("concurrent_counter_writes", 2)
                .set("concurrent_materialized_view_writes", 2)
                .set("concurrent_reads", 2)
                .set("memtable_flush_writers", 1)
                .set("concurrent_compactors", 1)
                .set("memtable_heap_space", new DataStorage("10mb"))
                .set("commitlog_sync", Config.CommitLogSync.batch)
                .set("storage_port", storage_port)
                .set("native_transport_port", native_transport_port)
                .set("endpoint_snitch", DistributedTestSnitch.class.getName())
                //.set("seed_provider", new ParameterizedClass(SimpleSeedProvider.class.getName(),
                        //Collections.singletonMap("seeds", seedIp + ":" + seedPort)))
                // required settings for dtest functionality
                .set("diagnostic_events_enabled", true)
                .set("auto_bootstrap", false)
                // capacities that are based on `totalMemory` that should be fixed size
                .set("index_summary_capacity", new DataStorage("50mb"))
                .set("counter_cache_size", new DataStorage("50mb"))
                .set("key_cache_size", new DataStorage("50mb"));
                // legacy parameters
                //.forceSet("commitlog_sync_batch_window", new Duration("1.0ms"));
        this.featureFlags = EnumSet.noneOf(Feature.class);
    }

    private InstanceConfig(InstanceConfig copy)
    {
        this.num = copy.num;
        this.networkTopology = new NetworkTopology(copy.networkTopology);
        this.params.putAll(copy.params);
        this.hostId = copy.hostId;
        this.featureFlags = copy.featureFlags;
        this.broadcastAddressAndPort = copy.broadcastAddressAndPort;
    }


    @Override
    public InetSocketAddress broadcastAddress()
    {
        return DistributedTestSnitch.fromCassandraInetAddressAndPort(getBroadcastAddressAndPort());
    }

    protected InetAddressAndPort getBroadcastAddressAndPort()
    {
        if (broadcastAddressAndPort == null)
        {
            broadcastAddressAndPort = getAddressAndPortFromConfig("broadcast_address", "storage_port");
        }
        return broadcastAddressAndPort;
    }

    private InetAddressAndPort getAddressAndPortFromConfig(String addressProp, String portProp)
    {
        try
        {
            return InetAddressAndPort.getByNameOverrideDefaults(getString(addressProp), getInt(portProp));
        }
        catch (UnknownHostException e)
        {
            throw new IllegalStateException(e);
        }
    }

    public String localRack()
    {
        return networkTopology().localRack(broadcastAddress());
    }

    public String localDatacenter()
    {
        return networkTopology().localDC(broadcastAddress());
    }

    public InstanceConfig with(Feature featureFlag)
    {
        featureFlags.add(featureFlag);
        return this;
    }

    public InstanceConfig with(Feature... flags)
    {
        for (Feature flag : flags)
            featureFlags.add(flag);
        return this;
    }

    public boolean has(Feature featureFlag)
    {
        return featureFlags.contains(featureFlag);
    }

    public InstanceConfig set(String fieldName, Object value)
    {
        if (value == null)
            value = NULL;

        params.put(fieldName, value);
        return this;
    }

    private InstanceConfig forceSet(String fieldName, Object value)
    {
        if (value == null)
            value = NULL;

        // test value
        params.put(fieldName, value);
        return this;
    }

    public void propagate(Object writeToConfig, Map<Class<?>, Function<Object, Object>> mapping)
    {
        /*for (Map.Entry<String, Object> e : params.entrySet())
            propagate(writeToConfig, e.getKey(), e.getValue(), mapping);*/
        Constructor constructor = new YamlConfigurationLoader.CustomConstructor(Config.class);
        Map<Class<?>, Map<String, YamlConfigurationLoader.Replacement>> replacements = getReplacements(Config.class);
        YamlConfigurationLoader.PropertiesChecker propertiesChecker = new YamlConfigurationLoader.PropertiesChecker(replacements);
        constructor.setPropertyUtils(propertiesChecker);
        constructor.setComposer(new Composer(null, null) {
            public Node getSingleNode()
            {
                return toNode(params);
            }
        });
        writeToConfig = constructor.getSingleData(Config.class);
    }

    public static Node toNode(Object object)
    {
        if (object instanceof Map)
        {
            List<NodeTuple> values = new ArrayList<>();
            for (Map.Entry<Object, Object> e : ((Map<Object, Object>) object).entrySet())
            {
                values.add(new NodeTuple(toNode(e.getKey()), toNode(e.getValue())));
            }
            return new MappingNode(FAKE_TAG, values, null);
        }
        else if (object instanceof Number || object instanceof String || object instanceof Boolean
         || object instanceof DataStorage || object instanceof Duration || object instanceof Config.CommitLogSync)
        {
            return new ScalarNode(Tag.STR, object.toString(), FAKE_MARK, FAKE_MARK, '\'');
        }
        else if(object instanceof String[])
        {
            int size = ((String[])object).length;
            Node values[] = new Node[size];
            SequenceNode node = new SequenceNode(Tag.STR, Arrays.asList(values), false);

            for (int i =0; i < size; i++)
                values[i] = new ScalarNode(Tag.STR, ((String[])object)[i], FAKE_MARK, FAKE_MARK, '\'');

            return node;
        }
        else
        {
            throw new UnsupportedOperationException("unexpected type found: given " + object.getClass());
        }
    }
    private static final Tag FAKE_TAG = new Tag("ignore");
    private static final Mark FAKE_MARK = new Mark("ignore", 0, 0, 0, "", 0);

    public void validate()
    {
        if (((int) get("num_tokens")) > 1)
            throw new IllegalArgumentException("In-JVM dtests do not support vnodes as of now.");
    }

    private void propagate(Object writeToConfig, String fieldName, Object value, Map<Class<?>, Function<Object, Object>> mapping)
    {
        if (value == NULL)
            value = null;

        if (mapping != null && mapping.containsKey(value.getClass()))
            value = mapping.get(value.getClass()).apply(value);

        Class<?> configClass = writeToConfig.getClass();
        Field valueField;
        try
        {
            valueField = configClass.getDeclaredField(fieldName);
        }
        catch (NoSuchFieldException e)
        {
            logger.warn("No such field: {} in config class {}", fieldName, configClass);
            return;
        }

        if (valueField.getType().isEnum() && value instanceof String)
        {
            String test = (String) value;
            value = Arrays.stream(valueField.getType().getEnumConstants())
                    .filter(e -> ((Enum<?>)e).name().equals(test))
                    .findFirst()
                    .get();
        }
        try
        {
            valueField.set(writeToConfig, value);
        }
        catch (IllegalAccessException | IllegalArgumentException e)
        {
            throw new IllegalStateException(e);
        }
    }

    public Object get(String name)
    {
        return params.get(name);
    }

    public final Map<String, Object> getParams()
    {
        return params;
    }

    public int getInt(String name)
    {
        return (Integer)params.get(name);
    }

    public String getString(String name)
    {
        return (String)params.get(name);
    }

    public static InstanceConfig generate(int nodeNum,
                                          INodeProvisionStrategy provisionStrategy,
                                          NetworkTopology networkTopology,
                                          File root,
                                          String token)
    {
        return new InstanceConfig(nodeNum,
                                  networkTopology,
                                  provisionStrategy.ipAddress(nodeNum),
                                  provisionStrategy.ipAddress(nodeNum),
                                  provisionStrategy.ipAddress(nodeNum),
                                  provisionStrategy.ipAddress(nodeNum),
                                  provisionStrategy.seedIp(),
                                  provisionStrategy.seedPort(),
                                  String.format("%s/node%d/saved_caches", root, nodeNum),
                                  new String[] { String.format("%s/node%d/data", root, nodeNum) },
                                  String.format("%s/node%d/commitlog", root, nodeNum),
                                  String.format("%s/node%d/hints", root, nodeNum),
                                  String.format("%s/node%d/cdc", root, nodeNum),
                                  token,
                                  provisionStrategy.storagePort(nodeNum),
                                  provisionStrategy.nativeTransportPort(nodeNum));
    }

    public InstanceConfig forVersion(Versions.Major major)
    {
        switch (major)
        {
            case v4: return this;
            default: return new InstanceConfig(this)
                            .set("seed_provider", new ParameterizedClass(SimpleSeedProvider.class.getName(),
                                                                         Collections.singletonMap("seeds", "127.0.0.1")));
        }
    }

    public String toString()
    {
        return params.toString();
    }
}

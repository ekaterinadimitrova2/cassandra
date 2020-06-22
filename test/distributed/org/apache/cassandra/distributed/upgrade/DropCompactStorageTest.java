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

package org.apache.cassandra.distributed.upgrade;

import org.junit.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.Versions;

public class DropCompactStorageTest extends UpgradeTestBase
{
    @Test
    public void dropCompactStorageBeforeUpgradesstablesTo30() throws Throwable
    {
        dropCompactStorageBeforeUpgradeSstables(Versions.Major.v30);
    }

    /**
     * Upgrades a node from 2.2 to 3.x and DROP COMPACT just after the upgrade but _before_ upgrading the underlying
     * sstables.
     *
     * <p>This test reproduces the issue from CASSANDRA-15897.
     */
    public void dropCompactStorageBeforeUpgradeSstables(Versions.Major upgradeTo) throws Throwable
    {
        new TestCase()
        .nodes(1)
        .upgrade(Versions.Major.v22, upgradeTo)
        .setup((cluster) -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (id int, ck int, v int, PRIMARY KEY (id, ck)) WITH COMPACT STORAGE");
            for (int i = 0; i < 5; i++)
                cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, ck, v) values (1, ?, ?)", ConsistencyLevel.ALL, i, i);
            cluster.get(1).flush(KEYSPACE);
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            cluster.schemaChange("ALTER TABLE "+KEYSPACE+".tbl DROP COMPACT STORAGE");
            cluster.coordinator(1).execute("SELECT * FROM "+KEYSPACE+".tbl", ConsistencyLevel.ALL);
        })
        .run();
    }
}

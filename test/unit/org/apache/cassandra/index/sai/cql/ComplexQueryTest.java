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

package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;

public class ComplexQueryTest extends SAITester
{
    @Test
    public void partialUpdateTest()
    {
        createTable("CREATE TABLE %s (pk int, c1 text, c2 text, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(c1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(c2) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, c1, c2) VALUES (?, ?, ?)", 1, "a", "a");
        flush();
        execute("UPDATE %s SET c1 = ? WHERE pk = ?", "b", 1);
        flush();
        execute("UPDATE %s SET c2 = ? WHERE pk = ?", "c", 1);
        flush();

        UntypedResultSet resultSet = execute("SELECT pk FROM %s WHERE c1 = 'b' AND c2='c'");
        assertRows(resultSet, row(1));
    }

    @Test
    public void splitRowsWithBooleanLogic()
    {
        createTable(KEYSPACE, "CREATE TABLE %s (pk int primary key, str_val text, val text)");
        createIndex("CREATE CUSTOM INDEX ON %s(str_val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        disableCompaction(KEYSPACE);

        // flush a sstable with 2 partial rows
        execute("INSERT INTO %s (pk, str_val) VALUES (3, 'A')");
        execute("INSERT INTO %s (pk, val) VALUES (1, 'A')");
        flush();

        // flush another sstable with 2 more partial rows, where PK 3 is now a complete row
        execute("INSERT INTO %s (pk, val) VALUES (3, 'A')");
        execute("INSERT INTO %s (pk, str_val) VALUES (2, 'A')");
        flush();

        // pk 3 should match
        var result = execute("SELECT pk FROM %s WHERE str_val = 'A' AND val = 'A'");
        assertRows(result, row(3));
    }

    @Test
    public void complexQueryWithMultipleNEQ()
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY(pk, ck))");
        createIndex("CREATE CUSTOM INDEX ON %s(a) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(b) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 1, 1, 5);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 2, 2, 6);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 3, 3, 7);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 4, 4, 8);
        execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, ?, ?)", 1, 5, null, null);

        assertRowsIgnoringOrder(execute("SELECT ck FROM %s WHERE pk = 1 AND a != 2 AND b != 7"), row(1), row(4));
        assertRowsIgnoringOrder(execute("SELECT ck FROM %s WHERE pk = 1 AND a != 2 AND a != 3"), row(1), row(4));
        assertRowsIgnoringOrder(execute("SELECT ck FROM %s WHERE pk = 1 AND a NOT IN (2, 3) ALLOW FILTERING"), row(1), row(4));
        assertRowsIgnoringOrder(execute("SELECT ck FROM %s WHERE pk = 1 AND a NOT IN (2, 3) AND b NOT IN (7, 8) ALLOW FILTERING"), row(1));
        assertInvalidRequestMessage("Cannot execute this query as it might involve data filtering and thus may have unpredictable performance." +
                                    " If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING",
                                    "SELECT ck FROM %s WHERE pk = 1 AND a NOT IN (2, 3)");
        assertInvalidRequestMessage("Cannot execute this query as it might involve data filtering and thus may have unpredictable performance." +
                                    " If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING",
                                    "SELECT ck FROM %s WHERE pk = 1 AND a NOT IN (2, 3) AND b NOT IN (7, 8)");
    }
}

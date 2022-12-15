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
package com.dtstack.chunjun.connector.binlog.util;

import com.dtstack.chunjun.connector.binlog.config.BinlogConfig;
import com.dtstack.chunjun.throwable.ChunJunException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class BinlogUtilTest {

    BinlogConfig binlogConfig;
    Connection conn;
    Statement statement;
    ResultSet resultSet;

    @Before
    public void setup() throws SQLException {
        binlogConfig = new BinlogConfig();
        conn = mock(Connection.class);
        statement = mock(Statement.class);
        resultSet = mock(ResultSet.class);
        when(conn.createStatement()).thenReturn(statement);
        when(statement.executeQuery(ArgumentMatchers.any())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);
    }

    @Test
    public void getDatabaseTableMapTest() {
        List<String> tableNameList =
                Arrays.asList("db1.table1", "db2.table2", "table3", "db2.table4");
        Map<String, List<String>> db_tables =
                BinlogUtil.getDatabaseTableMap(tableNameList, "defaultDatabase");
        System.out.println(db_tables);
        assert db_tables.get("db1").size() == 1;
        assert db_tables.get("db2").size() == 2;
        assert db_tables.get("defaultDatabase").size() == 1;
        assert db_tables.size() == 3;
    }

    @Test
    public void checkAndGetFilterInfoTest() throws ChunJunException {
        String filter = "defaultSchema\\.test.*";
        String[] filterInfo = BinlogUtil.checkAndAnalyzeFilter(filter);
        assert filterInfo.length == 2;
        assert filterInfo[0].equals("defaultSchema");
        assert filterInfo[1].equals("test.*");
    }
}

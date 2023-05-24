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

package com.dtstack.chunjun.connector.jdbc.converter;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.jdbc.adapter.ConnectionAdapter;
import com.dtstack.chunjun.connector.jdbc.config.ConnectionConfig;
import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.config.SourceConnectionConfig;
import com.dtstack.chunjun.connector.jdbc.exclusion.FieldNameExclusionStrategy;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Timestamp;

import static com.dtstack.chunjun.connector.jdbc.util.JdbcUtilTest.readFile;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class JdbcSyncConverterTest {

    private static JdbcSyncConverter converter;

    @BeforeClass
    public static void setup() throws IOException {
        String json = readFile("sync_test.json");
        SyncConfig syncConfig = SyncConfig.parseJob(json);
        Gson gson =
                new GsonBuilder()
                        .registerTypeAdapter(
                                ConnectionConfig.class,
                                new ConnectionAdapter(SourceConnectionConfig.class.getName()))
                        .addDeserializationExclusionStrategy(
                                new FieldNameExclusionStrategy("column"))
                        .create();
        GsonUtil.setTypeAdapter(gson);
        JdbcConfig jdbcConfig =
                gson.fromJson(gson.toJson(syncConfig.getReader().getParameter()), JdbcConfig.class);
        jdbcConfig.setColumn(syncConfig.getReader().getFieldList());

        RowType rowType =
                TableUtil.createRowType(jdbcConfig.getColumn(), JdbcRawTypeConverterTest::apply);
        converter = new JdbcSyncConverter(rowType, jdbcConfig);
    }

    @Test
    public void toInternalTest() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getObject(1)).thenReturn(1);
        when(resultSet.getObject(2)).thenReturn(true);
        when(resultSet.getObject(3)).thenReturn((byte) 1);
        when(resultSet.getObject(4)).thenReturn((short) 11);
        when(resultSet.getObject(5)).thenReturn(12);
        when(resultSet.getObject(6)).thenReturn(13L);
        when(resultSet.getObject(7)).thenReturn(14.14f);
        when(resultSet.getObject(8)).thenReturn(15.15d);
        when(resultSet.getObject(9)).thenReturn(new BigDecimal(10));
        when(resultSet.getObject(10)).thenReturn("asd");
        when(resultSet.getObject(11)).thenReturn(Date.valueOf("2022-01-01"));
        when(resultSet.getObject(12)).thenReturn(Timestamp.valueOf("2022-01-01 00:00:00"));
        RowData rowData = converter.toInternal(resultSet);

        FieldNamedPreparedStatement statement = mock(FieldNamedPreparedStatement.class);
        converter.toExternal(rowData, statement);
    }
}

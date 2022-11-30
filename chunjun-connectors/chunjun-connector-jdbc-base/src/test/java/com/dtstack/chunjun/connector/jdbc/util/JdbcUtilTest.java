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
package com.dtstack.chunjun.connector.jdbc.util;

import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.config.SourceConnectionConfig;
import com.dtstack.chunjun.constants.ConstantValue;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Objects;

/** @author dujie */
public class JdbcUtilTest {

    private JdbcConfig jdbcConfig;
    private SourceConnectionConfig sourceConnectionConf;

    @Before
    public void setup() {
        jdbcConfig = new JdbcConfig();
        sourceConnectionConf = new SourceConnectionConfig();
        jdbcConfig.setConnection(Collections.singletonList(sourceConnectionConf));
    }

    @Test
    public void testGetTableAndSchema1() {
        String schema = "\"schema\"";
        String table = "\"table\"";
        sourceConnectionConf.setTable(
                Lists.newArrayList(schema + ConstantValue.POINT_SYMBOL + table));
        JdbcUtil.resetSchemaAndTable(jdbcConfig, "\\\"", "\\\"");

        Assert.assertEquals("schema", jdbcConfig.getSchema());
        Assert.assertEquals("table", jdbcConfig.getTable());
    }

    @Test
    public void testGetTableAndSchema2() {
        String schema = "schema";
        String table = "table";
        sourceConnectionConf.setTable(
                Lists.newArrayList(schema + ConstantValue.POINT_SYMBOL + table));
        JdbcUtil.resetSchemaAndTable(jdbcConfig, "\\\"", "\\\"");

        Assert.assertEquals("schema", jdbcConfig.getSchema());
        Assert.assertEquals("table", jdbcConfig.getTable());
    }

    @Test
    public void testGetTableAndSchema3() {
        String schema = "[schema]";
        String table = "[table]";

        sourceConnectionConf.setTable(
                Lists.newArrayList(schema + ConstantValue.POINT_SYMBOL + table));
        JdbcUtil.resetSchemaAndTable(jdbcConfig, "\\[", "\\]");

        Assert.assertEquals("schema", jdbcConfig.getSchema());
        Assert.assertEquals("table", jdbcConfig.getTable());
    }

    @Test
    public void testGetTableAndSchema4() {
        String schema = "[[schema]";
        String table = "[table]";

        sourceConnectionConf.setTable(
                Lists.newArrayList(schema + ConstantValue.POINT_SYMBOL + table));
        JdbcUtil.resetSchemaAndTable(jdbcConfig, "\\[", "\\]");

        Assert.assertEquals("[schema", jdbcConfig.getSchema());
        Assert.assertEquals("table", jdbcConfig.getTable());
    }

    @Test
    public void testGetTableAndSchema5() {
        String schema = "[[sche.ma]]";
        String table = "[table]";

        sourceConnectionConf.setTable(
                Lists.newArrayList(schema + ConstantValue.POINT_SYMBOL + table));
        JdbcUtil.resetSchemaAndTable(jdbcConfig, "\\[", "\\]");

        Assert.assertEquals("[sche.ma]", jdbcConfig.getSchema());
        Assert.assertEquals("table", jdbcConfig.getTable());
    }

    public static String readFile(String fileName) throws IOException {
        // Creating an InputStream object
        try (InputStream inputStream =
                        Objects.requireNonNull(
                                JdbcUtilTest.class.getClassLoader().getResourceAsStream(fileName));
                // creating an InputStreamReader object
                InputStreamReader isReader = new InputStreamReader(inputStream);
                // Creating a BufferedReader object
                BufferedReader reader = new BufferedReader(isReader)) {
            StringBuilder sb = new StringBuilder();
            String str;
            while ((str = reader.readLine()) != null) {
                sb.append(str);
            }

            return sb.toString();
        }
    }
}

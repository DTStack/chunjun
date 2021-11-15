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
package com.dtstack.flinkx.connector.jdbc.util;

import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.conf.SourceConnectionConf;
import com.dtstack.flinkx.constants.ConstantValue;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

/** @author dujie */
public class JdbcUtilTest {

    private JdbcConf jdbcConf;
    private SourceConnectionConf sourceConnectionConf;

    @Before
    public void setup() {
        jdbcConf = new JdbcConf();
        sourceConnectionConf = new SourceConnectionConf();
        jdbcConf.setConnection(Collections.singletonList(sourceConnectionConf));
    }

    @Test
    public void testGetTableAndSchema1() {
        String schema = "\"schema\"";
        String table = "\"table\"";
        sourceConnectionConf.setTable(
                Lists.newArrayList(schema + ConstantValue.POINT_SYMBOL + table));
        JdbcUtil.resetSchemaAndTable(jdbcConf, "\\\"", "\\\"");

        Assert.assertEquals("schema", jdbcConf.getSchema());
        Assert.assertEquals("table", jdbcConf.getTable());
    }

    @Test
    public void testGetTableAndSchema2() {
        String schema = "schema";
        String table = "table";
        sourceConnectionConf.setTable(
                Lists.newArrayList(schema + ConstantValue.POINT_SYMBOL + table));
        JdbcUtil.resetSchemaAndTable(jdbcConf, "\\\"", "\\\"");

        Assert.assertEquals("schema", jdbcConf.getSchema());
        Assert.assertEquals("table", jdbcConf.getTable());
    }

    @Test
    public void testGetTableAndSchema3() {
        String schema = "[schema]";
        String table = "[table]";

        sourceConnectionConf.setTable(
                Lists.newArrayList(schema + ConstantValue.POINT_SYMBOL + table));
        JdbcUtil.resetSchemaAndTable(jdbcConf, "\\[", "\\]");

        Assert.assertEquals("schema", jdbcConf.getSchema());
        Assert.assertEquals("table", jdbcConf.getTable());
    }

    @Test
    public void testGetTableAndSchema4() {
        String schema = "[[schema]";
        String table = "[table]";

        sourceConnectionConf.setTable(
                Lists.newArrayList(schema + ConstantValue.POINT_SYMBOL + table));
        JdbcUtil.resetSchemaAndTable(jdbcConf, "\\[", "\\]");

        Assert.assertEquals("[schema", jdbcConf.getSchema());
        Assert.assertEquals("table", jdbcConf.getTable());
    }

    @Test
    public void testGetTableAndSchema5() {
        String schema = "[[sche.ma]]";
        String table = "[table]";

        sourceConnectionConf.setTable(
                Lists.newArrayList(schema + ConstantValue.POINT_SYMBOL + table));
        JdbcUtil.resetSchemaAndTable(jdbcConf, "\\[", "\\]");

        Assert.assertEquals("[sche.ma]", jdbcConf.getSchema());
        Assert.assertEquals("table", jdbcConf.getTable());
    }
}

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

package com.dtstack.chunjun.connector.jdbc.dialect;

import com.dtstack.chunjun.connector.jdbc.source.JdbcInputSplit;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({JdbcDialect.class, JdbcInputSplit.class})
public class JdbcDialectTest {
    private static JdbcDialect jdbcDialect;
    private static String schema = "schema";
    private static String table = "table";
    private static String[] fields = new String[] {"id", "name"};
    private static String[] nullFields = new String[] {};
    private static String[] conditionFields = new String[] {"id"};

    @BeforeClass
    public static void setup() {
        jdbcDialect = mock(JdbcDialect.class);
        when(jdbcDialect.quoteIdentifier(anyString())).thenCallRealMethod();
        when(jdbcDialect.buildTableInfoWithSchema(schema, table)).thenCallRealMethod();
    }

    @Test
    public void getSqlQueryFieldsTest() {
        String expect;

        when(jdbcDialect.getSqlQueryFields(anyString(), anyString())).thenCallRealMethod();
        expect = "SELECT * FROM \"schema\".\"table\" LIMIT 0";
        Assert.assertEquals(expect, jdbcDialect.getSqlQueryFields(schema, table));

        when(jdbcDialect.getSqlQueryFields(null, table)).thenCallRealMethod();
        when(jdbcDialect.buildTableInfoWithSchema(null, table)).thenCallRealMethod();
        expect = "SELECT * FROM \"table\" LIMIT 0";
        Assert.assertEquals(expect, jdbcDialect.getSqlQueryFields(null, table));
    }

    @Test
    public void quoteTableTest() {
        when(jdbcDialect.quoteTable(anyString())).thenCallRealMethod();
        String expect = "\"schema\".\"table\"";
        Assert.assertEquals(expect, jdbcDialect.quoteTable("schema.table"));
    }

    @Test
    public void getRowExistsStatementTest() {
        when(jdbcDialect.getRowExistsStatement(schema, table, fields)).thenCallRealMethod();
        String expect = "SELECT 1 FROM \"schema\".\"table\" WHERE \"id\" = ? AND \"name\" = ?";
        Assert.assertEquals(expect, jdbcDialect.getRowExistsStatement(schema, table, fields));
    }

    @Test
    public void getUpdateStatementTest() {
        when(jdbcDialect.getUpdateStatement(schema, table, fields, conditionFields))
                .thenCallRealMethod();
        String expect = "UPDATE \"schema\".\"table\" SET \"name\" = :name WHERE \"id\" = :id";
        Assert.assertEquals(
                expect, jdbcDialect.getUpdateStatement(schema, table, fields, conditionFields));
    }

    @Test
    public void getDeleteStatementTest() {
        when(jdbcDialect.getDeleteStatement(schema, table, fields, nullFields))
                .thenCallRealMethod();
        String expect = "DELETE FROM \"schema\".\"table\" WHERE \"id\" = :id AND \"name\" = :name";
        Assert.assertEquals(
                expect, jdbcDialect.getDeleteStatement(schema, table, fields, nullFields));
    }

    @Test
    public void getSelectFromStatementTest1() {
        when(jdbcDialect.getSelectFromStatement(schema, table, fields, conditionFields))
                .thenCallRealMethod();
        String expect = "SELECT \"id\", \"name\" FROM \"schema\".\"table\" WHERE \"id\" = ?";
        Assert.assertEquals(
                expect, jdbcDialect.getSelectFromStatement(schema, table, fields, conditionFields));
    }

    @Test
    public void getSelectFromStatement2() {
        String customSql = "select id,name from \"schema\".\"table\"";
        String where = "id > 10";
        String expect;

        // custom and where
        when(jdbcDialect.getSelectFromStatement(schema, table, customSql, conditionFields, where))
                .thenCallRealMethod();
        expect =
                "SELECT * FROM (select id,name from \"schema\".\"table\") chunjun_tmp WHERE id > 10";
        Assert.assertEquals(
                expect,
                jdbcDialect.getSelectFromStatement(
                        schema, table, customSql, conditionFields, where));

        // custom
        when(jdbcDialect.getSelectFromStatement(schema, table, customSql, conditionFields, null))
                .thenCallRealMethod();
        expect = "SELECT * FROM (select id,name from \"schema\".\"table\") chunjun_tmp WHERE  1=1 ";
        Assert.assertEquals(
                expect,
                jdbcDialect.getSelectFromStatement(
                        schema, table, customSql, conditionFields, null));

        // where
        when(jdbcDialect.getSelectFromStatement(schema, table, null, conditionFields, where))
                .thenCallRealMethod();
        expect = "SELECT \"id\" FROM \"schema\".\"table\" WHERE id > 10";
        Assert.assertEquals(
                expect,
                jdbcDialect.getSelectFromStatement(schema, table, null, conditionFields, where));

        // normal
        when(jdbcDialect.getSelectFromStatement(schema, table, null, conditionFields, null))
                .thenCallRealMethod();
        expect = "SELECT \"id\" FROM \"schema\".\"table\" WHERE  1=1 ";
        Assert.assertEquals(
                expect,
                jdbcDialect.getSelectFromStatement(schema, table, null, conditionFields, null));
    }

    @Test
    public void getSelectFromStatement3() {
        String customSql = "select id,name from \"schema\".\"table\"";
        String where = "id > 10";
        String expect;

        // custom and where
        when(jdbcDialect.getSelectFromStatement(
                        schema, table, customSql, fields, conditionFields, where))
                .thenCallRealMethod();
        expect =
                "SELECT * FROM (select id,name from \"schema\".\"table\") chunjun_tmp WHERE id > 10";
        Assert.assertEquals(
                expect,
                jdbcDialect.getSelectFromStatement(
                        schema, table, customSql, fields, conditionFields, where));

        // custom
        when(jdbcDialect.getSelectFromStatement(
                        schema, table, customSql, fields, conditionFields, null))
                .thenCallRealMethod();
        expect = "SELECT * FROM (select id,name from \"schema\".\"table\") chunjun_tmp WHERE  1=1 ";
        Assert.assertEquals(
                expect,
                jdbcDialect.getSelectFromStatement(
                        schema, table, customSql, fields, conditionFields, null));

        // where
        when(jdbcDialect.getSelectFromStatement(
                        schema, table, null, fields, conditionFields, where))
                .thenCallRealMethod();
        expect = "SELECT \"id\", \"name\", id FROM \"schema\".\"table\" WHERE id > 10";
        Assert.assertEquals(
                expect,
                jdbcDialect.getSelectFromStatement(
                        schema, table, null, fields, conditionFields, where));

        // normal
        when(jdbcDialect.getSelectFromStatement(schema, table, null, fields, conditionFields, null))
                .thenCallRealMethod();
        expect = "SELECT \"id\", \"name\", id FROM \"schema\".\"table\" WHERE  1=1 ";
        Assert.assertEquals(
                expect,
                jdbcDialect.getSelectFromStatement(
                        schema, table, null, fields, conditionFields, null));
    }

    @Test
    public void getRowNumColumnTest() {
        when(jdbcDialect.getRowNumColumn(anyString())).thenCallRealMethod();
        Assert.assertThrows(
                UnsupportedOperationException.class, () -> jdbcDialect.getRowNumColumn("id"));
    }

    @Test
    public void getRowNumColumnAliasTest() {
        when(jdbcDialect.getRowNumColumnAlias()).thenCallRealMethod();
        Assert.assertEquals("CHUNJUN_ROWNUM", jdbcDialect.getRowNumColumnAlias());
    }

    @Test
    public void getSplitRangeFilterTest() {
        JdbcInputSplit jdbcInputSplit = mock(JdbcInputSplit.class);
        when(jdbcInputSplit.getStartLocationOfSplit()).thenReturn("10");
        when(jdbcInputSplit.getEndLocationOfSplit()).thenReturn("20");
        when(jdbcInputSplit.getRangeEndLocationOperator()).thenReturn(" < ");
        when(jdbcDialect.getSplitRangeFilter(jdbcInputSplit, "id")).thenCallRealMethod();

        String expect = "\"id\" >= 10 AND \"id\" < 20";
        Assert.assertEquals(expect, jdbcDialect.getSplitRangeFilter(jdbcInputSplit, "id"));
    }

    @Test
    public void getSplitModFilterTest() {
        JdbcInputSplit jdbcInputSplit = mock(JdbcInputSplit.class);
        when(jdbcInputSplit.getTotalNumberOfSplits()).thenReturn(10);
        when(jdbcInputSplit.getMod()).thenReturn(3);
        when(jdbcDialect.getSplitModFilter(jdbcInputSplit, "id")).thenCallRealMethod();

        String expect = " mod(\"id\", 10) = 3";
        Assert.assertEquals(expect, jdbcDialect.getSplitModFilter(jdbcInputSplit, "id"));
    }
}

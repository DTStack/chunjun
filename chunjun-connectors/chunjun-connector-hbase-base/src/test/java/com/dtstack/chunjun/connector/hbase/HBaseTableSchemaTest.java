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

package com.dtstack.chunjun.connector.hbase;

import com.dtstack.chunjun.connector.hbase.config.HBaseConfig;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class HBaseTableSchemaTest {

    private final HBaseTableSchema schema = new HBaseTableSchema();

    @Before
    public void setUp() {
        schema.setTableName("Test");
        schema.setCharset(StandardCharsets.UTF_8.name());
        schema.setRowKey("stu:id", DataTypes.INT());
    }

    @Test
    public void testGetTableName() {
        Assert.assertEquals("Test", schema.getTableName());
    }

    @Test
    public void testGetStringCharset() {
        Assert.assertEquals(StandardCharsets.UTF_8.name(), schema.getStringCharset());
    }

    @Test
    public void testSetRowKeyWithClass() {
        HBaseTableSchema tableSchema = new HBaseTableSchema();
        tableSchema.setRowKey("stu:id", Integer.class);
        DataType dataType = tableSchema.getRowKeyDataType().orElse(DataTypes.NULL());
        Assert.assertEquals(DataTypes.INT(), dataType);
        TableSchema convertedTableSchema = tableSchema.convertsToTableSchema();
        Assert.assertEquals("stu:id", convertedTableSchema.getFieldNames()[0]);
    }

    @Test
    public void testAddColumnWithClass() {
        HBaseTableSchema tableSchema = new HBaseTableSchema();
        tableSchema.addColumn("stu", "id", Integer.class);
        Map<String, DataType> stu = tableSchema.getFamilyInfo("stu");
        Assert.assertEquals(DataTypes.INT(), stu.get("id"));

        Assert.assertEquals("stu", tableSchema.getFamilyNames()[0]);
        Assert.assertEquals("stu", new String(tableSchema.getFamilyKeys()[0]));

        Assert.assertEquals("id", tableSchema.getQualifierNames("stu")[0]);
        Assert.assertEquals("id", new String(tableSchema.getQualifierKeys("stu")[0]));

        Assert.assertEquals(DataTypes.INT(), tableSchema.getQualifierDataTypes("stu")[0]);

        Assert.assertEquals(DataTypes.INT(), tableSchema.getFamilyInfo("stu").get("id"));

        TableSchema convertedTableSchema = tableSchema.convertsToTableSchema();
        Assert.assertEquals("stu", convertedTableSchema.getFieldNames()[0]);
    }

    @Test
    public void testFromTableSchema() {
        TableSchema tableSchema =
                TableSchema.builder()
                        .field("rowkey", DataTypes.INT())
                        .field("stu", DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT())))
                        .build();

        HBaseTableSchema fromTableSchema =
                HBaseTableSchema.fromTableSchema(tableSchema, new HBaseConfig());
        Assert.assertEquals("stu", fromTableSchema.getFamilyNames()[0]);
        Assert.assertEquals(
                DataTypes.INT(), fromTableSchema.getRowKeyDataType().orElse(DataTypes.NULL()));
    }
}

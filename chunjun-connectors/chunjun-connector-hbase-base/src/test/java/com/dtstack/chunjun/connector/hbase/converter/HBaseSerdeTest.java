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

package com.dtstack.chunjun.connector.hbase.converter;

import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;
import com.dtstack.chunjun.connector.hbase.config.HBaseConfig;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class HBaseSerdeTest {

    private HBaseTableSchema tableSchema;

    private HBaseSerde serde;

    @Before
    public void setUp() {
        tableSchema = new HBaseTableSchema();
        tableSchema.setRowKey("stu:id", Integer.class);
        tableSchema.addColumn("stu", "name", String.class);
        HBaseConfig hBaseConfig = new HBaseConfig();
        hBaseConfig.setNullStringLiteral("null");
        serde = new HBaseSerde(tableSchema, hBaseConfig);
    }

    @Test
    public void testCreatePutMutation() throws Exception {
        GenericRowData data = new GenericRowData(1);
        data.setField(0, StringData.fromString("hbase_test"));
        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, 1);
        rowData.setField(1, data);

        Put put = serde.createPutMutation(rowData);
        Delete delete = serde.createDeleteMutation(rowData);

        assert put != null;
        byte[] row = put.getRow();
        Assert.assertEquals(1, ByteBuffer.wrap(row).getInt());

        assert delete != null;
        byte[] deleteRow = delete.getRow();
        Assert.assertEquals(1, ByteBuffer.wrap(deleteRow).getInt());

        Assert.assertEquals(1, serde.getRowKey(deleteRow));

        Assert.assertEquals(1, ByteBuffer.wrap(serde.getRowKey(1)).getInt());
    }
}

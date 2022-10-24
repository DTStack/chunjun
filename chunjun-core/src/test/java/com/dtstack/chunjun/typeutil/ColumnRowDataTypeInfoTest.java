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

package com.dtstack.chunjun.typeutil;

import com.dtstack.chunjun.typeutil.serializer.ColumnRowDataSerializer;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Objects;

public class ColumnRowDataTypeInfoTest {

    private static ColumnRowDataTypeInfo<RowData> columnRowDataTypeInfo;

    @BeforeAll
    public static void setup() {
        RowType rowType = RowType.of(new IntType());
        columnRowDataTypeInfo = ColumnRowDataTypeInfo.of(rowType);
    }

    @Test
    public void testGetArity() {
        Assertions.assertEquals(1, columnRowDataTypeInfo.getArity());
    }

    @Test
    public void testGetTotalFields() {
        Assertions.assertEquals(1, columnRowDataTypeInfo.getTotalFields());
    }

    @Test
    public void testGetTypeClass() {
        Assertions.assertEquals(RowData.class, columnRowDataTypeInfo.getTypeClass());
    }

    @Test
    public void testIsTupleType() {
        Assertions.assertFalse(columnRowDataTypeInfo.isTupleType());
    }

    @Test
    public void testIsBasicType() {
        Assertions.assertFalse(columnRowDataTypeInfo.isBasicType());
    }

    @Test
    public void testIsKeyType() {
        Assertions.assertFalse(columnRowDataTypeInfo.isKeyType());
    }

    @Test
    public void testIsSortKeyType() {
        Assertions.assertFalse(columnRowDataTypeInfo.isSortKeyType());
    }

    @Test
    public void testCreateSerializer() {
        columnRowDataTypeInfo.createSerializer(new ExecutionConfig());
    }

    @Test
    public void testCanEqual() {
        RowType rowType = RowType.of(new BigIntType());
        ColumnRowDataTypeInfo<RowData> different =
                new ColumnRowDataTypeInfo<>(
                        rowType, RowData.class, new ColumnRowDataSerializer(rowType));
        Assertions.assertTrue(columnRowDataTypeInfo.canEqual(different));

        RowTypeInfo rowTypeInfo = new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO);
        Assertions.assertFalse(columnRowDataTypeInfo.canEqual(rowTypeInfo));
    }

    @Test
    public void testEqual() {
        RowType rowType = RowType.of(new BigIntType());
        ColumnRowDataTypeInfo<RowData> different =
                new ColumnRowDataTypeInfo<>(
                        rowType, RowData.class, new ColumnRowDataSerializer(rowType));
        Assertions.assertNotEquals(columnRowDataTypeInfo, different);
        Assertions.assertNotEquals(columnRowDataTypeInfo, null);
        Assertions.assertEquals(columnRowDataTypeInfo, columnRowDataTypeInfo);
    }

    @Test
    public void testToString() {
        Assertions.assertEquals(
                "ROW<`f0` INT>(org.apache.flink.table.data.RowData, com.dtstack.chunjun.typeutil.serializer.ColumnRowDataSerializer)",
                columnRowDataTypeInfo.toString());
    }

    @Test
    public void testHashCode() {
        TypeSerializer<RowData> typeSerializer =
                columnRowDataTypeInfo.createSerializer(new ExecutionConfig());
        Assertions.assertEquals(Objects.hash(typeSerializer), columnRowDataTypeInfo.hashCode());
    }
}

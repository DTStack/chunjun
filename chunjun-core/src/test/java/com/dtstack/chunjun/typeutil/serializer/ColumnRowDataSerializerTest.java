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

package com.dtstack.chunjun.typeutil.serializer;

import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.ByteColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.typeutil.serializer.base.BooleanColumnSerializerTest;
import com.dtstack.chunjun.typeutil.serializer.base.ByteColumnSerializerTest;
import com.dtstack.chunjun.typeutil.serializer.base.BytesColumnSerializerTest;
import com.dtstack.chunjun.typeutil.serializer.base.DecimalColumnSerializerTest;
import com.dtstack.chunjun.typeutil.serializer.base.DoubleColumnSerializerTest;
import com.dtstack.chunjun.typeutil.serializer.base.FloatColumnSerializerTest;
import com.dtstack.chunjun.typeutil.serializer.base.IntColumnSerializerTest;
import com.dtstack.chunjun.typeutil.serializer.base.LongColumnSerializerTest;
import com.dtstack.chunjun.typeutil.serializer.base.NullColumnSerializerTest;
import com.dtstack.chunjun.typeutil.serializer.base.ShortColumnSerializerTest;
import com.dtstack.chunjun.typeutil.serializer.base.SqlDateColumnSerializerTest;
import com.dtstack.chunjun.typeutil.serializer.base.StringColumnSerializerTest;
import com.dtstack.chunjun.typeutil.serializer.base.TimeColumnSerializerTest;
import com.dtstack.chunjun.typeutil.serializer.base.TimestampColumnSerializerTest;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;


public class ColumnRowDataSerializerTest extends SerializerTestBase<RowData> {

    private final DeeplyEqualsChecker.CustomEqualityChecker[] customEqualityCheckers = {
        new BooleanColumnSerializerTest.BooleanColumnChecker(),
        new BooleanColumnSerializerTest.BooleanColumnChecker(),
        new ByteColumnSerializerTest.ByteColumnChecker(),
        new ByteColumnSerializerTest.ByteColumnChecker(),
        new BytesColumnSerializerTest.BytesColumnChecker(),
        new DecimalColumnSerializerTest.DecimalColumnChecker(),
        new DoubleColumnSerializerTest.DoubleColumnChecker(),
        new FloatColumnSerializerTest.FloatColumnChecker(),
        new IntColumnSerializerTest.IntColumnChecker(),
        new LongColumnSerializerTest.LongColumnChecker(),
        new ShortColumnSerializerTest.ShortColumnChecker(),
        new SqlDateColumnSerializerTest.SqlDateColumnChecker(),
        new SqlDateColumnSerializerTest.SqlDateColumnChecker(),
        new StringColumnSerializerTest.StringColumnChecker(),
        new TimeColumnSerializerTest.TimeColumnChecker(),
        new TimestampColumnSerializerTest.TimestampColumnChecker(),
        new NullColumnSerializerTest.NullColumnChecker()
    };

    @Override
    protected Tuple2<BiFunction<Object, Object, Boolean>, DeeplyEqualsChecker.CustomEqualityChecker>
            getCustomChecker() {
        return Tuple2.of(
                new BiFunction<Object, Object, Boolean>() {
                    @Override
                    public Boolean apply(Object o, Object o2) {
                        return o instanceof ColumnRowData && o2 instanceof ColumnRowData;
                    }
                },
                new ColumnRowDataColumnChecker());
    }

    @Override
    protected TypeSerializer<RowData> createSerializer() {
        return new ColumnRowDataSerializer(getRowType());
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<RowData> getTypeClass() {
        return RowData.class;
    }

    @Override
    protected RowData[] getTestData() {
        return new RowData[] {getColumnRowData(), getColumnRowData()};
    }

    public class ColumnRowDataColumnChecker implements DeeplyEqualsChecker.CustomEqualityChecker {
        @Override
        public boolean check(Object o1, Object o2, DeeplyEqualsChecker checker) {
            if (o1 instanceof ColumnRowData && o2 instanceof ColumnRowData) {
                ColumnRowData want = (ColumnRowData) o1;
                ColumnRowData that = (ColumnRowData) o2;
                if (want.getRowKind() != that.getRowKind()
                        || want.getHeaderInfo().size() != that.getHeaderInfo().size()
                        || want.getExtHeader().size() != that.getExtHeader().size()
                        || want.getArity() != that.getArity()
                        || want.getByteSize() != that.getByteSize()) {
                    return false;
                }
                for (Map.Entry<String, Integer> entry : want.getHeaderInfo().entrySet()) {
                    if (!Objects.equals(
                            entry.getValue(), that.getHeaderInfo().get(entry.getKey()))) {
                        return false;
                    }
                }
                for (String extStr : want.getExtHeader()) {
                    if (!that.getExtHeader().contains(extStr)) {
                        return false;
                    }
                }
                for (int i = 0; i < want.getArity(); i++) {
                    AbstractBaseColumn wantFiled = want.getField(i);
                    AbstractBaseColumn thatField = that.getField(i);
                    if (!customEqualityCheckers[i].check(wantFiled, thatField, null)) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    public RowType getRowType() {
        List<RowType.RowField> rowFieldList = new ArrayList<>(13);
        rowFieldList.add(new RowType.RowField("BooleanData", DataTypes.BOOLEAN().getLogicalType()));
        rowFieldList.add(
                new RowType.RowField("BooleanData2", DataTypes.BOOLEAN().getLogicalType()));
        rowFieldList.add(new RowType.RowField("ByteData", DataTypes.TINYINT().getLogicalType()));
        rowFieldList.add(new RowType.RowField("ByteData2", DataTypes.TINYINT().getLogicalType()));
        rowFieldList.add(new RowType.RowField("BytesData", DataTypes.BYTES().getLogicalType()));
        rowFieldList.add(
                new RowType.RowField("DecimalData", DataTypes.DECIMAL(1, 1).getLogicalType()));
        rowFieldList.add(new RowType.RowField("DoubleData", DataTypes.DOUBLE().getLogicalType()));
        rowFieldList.add(new RowType.RowField("FloatData", DataTypes.FLOAT().getLogicalType()));
        rowFieldList.add(new RowType.RowField("IntData", DataTypes.INT().getLogicalType()));
        rowFieldList.add(new RowType.RowField("LongData", DataTypes.BIGINT().getLogicalType()));
        rowFieldList.add(new RowType.RowField("ShortData", DataTypes.SMALLINT().getLogicalType()));
        rowFieldList.add(new RowType.RowField("SqlDateData", DataTypes.DATE().getLogicalType()));
        rowFieldList.add(new RowType.RowField("SqlDateData2", DataTypes.DATE().getLogicalType()));
        rowFieldList.add(
                new RowType.RowField(
                        "StringData",
                        DataTypes.STRING().getLogicalType(),
                        "yyyy-MM-dd HH:mm:ss.SSS"));
        rowFieldList.add(new RowType.RowField("TimeData", DataTypes.TIME().getLogicalType()));
        rowFieldList.add(
                new RowType.RowField("TimestampData", DataTypes.TIMESTAMP().getLogicalType()));
        rowFieldList.add(new RowType.RowField("NullColumn", DataTypes.NULL().getLogicalType()));
        return new RowType(rowFieldList);
    }

    public ColumnRowData getColumnRowData() {
        ColumnRowData columnRowData = new ColumnRowData(13);
        columnRowData.addHeader("123");
        columnRowData.addExtHeader("1234");
        columnRowData.addField(new BooleanColumn(false));
        columnRowData.addField(new BytesColumn(new byte[] {1, 2}));
        columnRowData.addField(new ByteColumn((byte) 1));
        columnRowData.addField(new BigDecimalColumn((byte) 1));
        columnRowData.addField(new BytesColumn((new byte[] {1, 2})));
        columnRowData.addField(new BigDecimalColumn(new BigDecimal("1234123123")));
        columnRowData.addField(new BigDecimalColumn(123.123));
        columnRowData.addField(new BigDecimalColumn((float) 1.12));
        columnRowData.addField(new BigDecimalColumn(123));
        columnRowData.addField(new BigDecimalColumn(123123123L));
        columnRowData.addField(new BigDecimalColumn((short) 12));
        columnRowData.addField(new SqlDateColumn(100));
        columnRowData.addField(new TimestampColumn(System.currentTimeMillis(), 0));
        columnRowData.addField(new StringColumn("123", "yyyy-MM-dd HH:mm:ss.SSS"));
        columnRowData.addField(new TimeColumn(19));
        columnRowData.addField(new TimestampColumn(System.currentTimeMillis()));
        columnRowData.addField(new NullColumn());
        return columnRowData;
    }
}

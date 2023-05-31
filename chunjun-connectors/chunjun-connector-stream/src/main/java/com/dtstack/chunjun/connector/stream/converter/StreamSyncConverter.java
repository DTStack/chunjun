/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.stream.converter;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.ByteColumn;
import com.dtstack.chunjun.element.column.DoubleColumn;
import com.dtstack.chunjun.element.column.FloatColumn;
import com.dtstack.chunjun.element.column.IntColumn;
import com.dtstack.chunjun.element.column.LongColumn;
import com.dtstack.chunjun.element.column.ShortColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;

import org.apache.flink.table.data.RowData;

import com.github.jsonzou.jmockdata.JMockData;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class StreamSyncConverter
        extends AbstractRowConverter<ColumnRowData, RowData, RowData, String> {

    private static final long serialVersionUID = -3451666548578322227L;

    private static final AtomicLong id = new AtomicLong(0L);

    public StreamSyncConverter(CommonConfig commonConfig) {
        List<TypeConfig> typeList =
                commonConfig.getColumn().stream()
                        .map(FieldConfig::getType)
                        .collect(Collectors.toList());
        super.commonConfig = commonConfig;
        toInternalConverters = new ArrayList<>(typeList.size());
        toExternalConverters = new ArrayList<>(typeList.size());

        for (TypeConfig s : typeList) {
            toInternalConverters.add(createInternalConverter(s.getType()));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(s.getType()), s.getType()));
        }
    }

    @Override
    @SuppressWarnings("all")
    protected ISerializationConverter<ColumnRowData> wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, String type) {
        return (val, index, rowData) -> rowData.addField(((ColumnRowData) val).getField(index));
    }

    @Override
    protected IDeserializationConverter<RowData, AbstractBaseColumn> createInternalConverter(
            String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "ID":
                return val -> new LongColumn(id.incrementAndGet());
            case "INT":
            case "INTEGER":
                return val -> new IntColumn(JMockData.mock(int.class));
            case "YEAR":
                return val -> new IntColumn(1997);
            case "BOOLEAN":
                return val -> new BooleanColumn(JMockData.mock(boolean.class));
            case "TINYINT":
            case "BYTE":
                return val -> new ByteColumn(JMockData.mock(byte.class));
            case "CHAR":
            case "CHARACTER":
                return val -> new StringColumn(JMockData.mock(char.class).toString());
            case "SHORT":
            case "SMALLINT":
                return val -> new ShortColumn(JMockData.mock(short.class));
            case "LONG":
            case "BIGINT":
                return val -> new LongColumn(JMockData.mock(long.class));
            case "FLOAT":
                return val -> new FloatColumn(JMockData.mock(float.class));
            case "DOUBLE":
                return val -> new DoubleColumn(JMockData.mock(double.class));
            case "DECIMAL":
                return val -> new BigDecimalColumn(JMockData.mock(BigDecimal.class));
            case "DATE":
                return val -> new SqlDateColumn(Date.valueOf(LocalDate.now()));
            case "DATETIME":
                return val -> new TimestampColumn(System.currentTimeMillis(), 0);
            case "TIMESTAMP":
                return val -> new TimestampColumn(System.currentTimeMillis());
            case "TIME":
                return val -> new TimeColumn(Time.valueOf(LocalTime.now()));
            default:
                return val -> new StringColumn(JMockData.mock(String.class));
        }
    }

    @Override
    protected ISerializationConverter<ColumnRowData> createExternalConverter(String type) {
        return (val, index, rowData) -> rowData.addField(((ColumnRowData) val).getField(index));
    }

    @Override
    @SuppressWarnings("unchecked")
    public ColumnRowData toInternal(ColumnRowData rowData) throws Exception {
        List<FieldConfig> fieldConfigList = commonConfig.getColumn();
        ColumnRowData result = new ColumnRowData(fieldConfigList.size());
        for (int i = 0; i < fieldConfigList.size(); i++) {
            AbstractBaseColumn baseColumn =
                    (AbstractBaseColumn) toInternalConverters.get(i).deserialize(null);
            result.addField(assembleFieldProps(fieldConfigList.get(i), baseColumn));
        }
        return result;
    }

    @Override
    public RowData toExternal(RowData rowData, RowData output) {
        return rowData;
    }
}

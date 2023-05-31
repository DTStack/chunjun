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

package com.dtstack.chunjun.connector.ftp.converter;

import com.dtstack.chunjun.connector.ftp.config.FtpConfig;
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
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;
import com.dtstack.chunjun.util.DateUtil;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class FtpSyncConverter extends AbstractRowConverter<RowData, RowData, String, LogicalType> {

    private static final long serialVersionUID = -2759374929827259886L;

    private final FtpConfig ftpConfig;

    public FtpSyncConverter(RowType rowType, FtpConfig ftpConfig) {
        super(rowType, ftpConfig);
        this.ftpConfig = ftpConfig;
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(rowType.getTypeAt(i)), rowType.getTypeAt(i)));
        }
    }

    @Override
    public RowData toInternal(RowData input) throws Exception {
        ColumnRowData row = new ColumnRowData(input.getArity());
        if (input instanceof GenericRowData) {
            GenericRowData genericRowData = (GenericRowData) input;
            for (int i = 0; i < input.getArity(); i++) {
                row.addField(
                        (AbstractBaseColumn)
                                toInternalConverters
                                        .get(i)
                                        .deserialize(genericRowData.getField(i)));
            }
        } else {
            throw new ChunJunRuntimeException(
                    "Error RowData type, RowData:["
                            + input
                            + "] should be instance of GenericRowData.");
        }
        return row;
    }

    @Override
    public String toExternal(RowData rowData, String output) throws Exception {
        StringBuilder sb = new StringBuilder(128);

        List<String> columnData = new ArrayList<>(ftpConfig.getColumn().size());
        for (int index = 0; index < fieldTypes.length; index++) {
            toExternalConverters.get(index).serialize(rowData, index, columnData);
            if (index != 0) {
                sb.append(ftpConfig.getFieldDelimiter());
            }
            sb.append(columnData.get(index));
        }
        return sb.toString();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ISerializationConverter<List<String>> wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, LogicalType logicalType) {
        return (rowData, index, list) -> {
            if (rowData == null || rowData.isNullAt(index)) {
                list.add(index, null);
            } else {
                serializationConverter.serialize(rowData, index, list);
            }
        };
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new BooleanColumn(Boolean.parseBoolean(val));
            case TINYINT:
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new ByteColumn(Byte.parseByte(val));
            case SMALLINT:
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new ShortColumn(Short.parseShort(val));
            case INTEGER:
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new IntColumn(Integer.parseInt(val));
            case BIGINT:
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new LongColumn(Long.parseLong(val));
            case FLOAT:
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new FloatColumn(Float.parseFloat(val));
            case DOUBLE:
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new DoubleColumn(Double.parseDouble(val));
            case DECIMAL:
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        BigDecimalColumn::new;
            case VARCHAR:
            case CHAR:
                return (IDeserializationConverter<String, AbstractBaseColumn>) StringColumn::new;
            case TIME_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new TimeColumn(Time.valueOf(val));
            case DATE:
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new SqlDateColumn(Date.valueOf(val));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<Object, AbstractBaseColumn>)
                        val -> {
                            int precision = ((TimestampType) (type)).getPrecision();
                            Timestamp timestamp = DateUtil.convertToTimestamp(val.toString());
                            if (timestamp != null) {
                                return new TimestampColumn(timestamp, precision);
                            }
                            return new TimestampColumn(
                                    DateUtil.getTimestampFromStr(val.toString()), precision);
                        };
            default:
                throw new UnsupportedTypeException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<List<String>> createExternalConverter(
            LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case DATE:
                return (rowData, index, list) -> {
                    if (rowData instanceof ColumnRowData) {
                        list.add(
                                index,
                                ((ColumnRowData) rowData).getField(index).asSqlDate().toString());
                    } else {
                        list.add(index, ((GenericRowData) rowData).getField(index).toString());
                    }
                };
            default:
                return (rowData, index, list) -> {
                    if (rowData instanceof ColumnRowData) {
                        list.add(index, ((ColumnRowData) rowData).getField(index).asString());
                    } else {
                        list.add(index, ((GenericRowData) rowData).getField(index).toString());
                    }
                };
        }
    }
}

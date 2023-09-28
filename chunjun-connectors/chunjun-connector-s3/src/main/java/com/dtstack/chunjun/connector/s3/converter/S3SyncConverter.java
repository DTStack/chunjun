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

package com.dtstack.chunjun.connector.s3.converter;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.DoubleColumn;
import com.dtstack.chunjun.element.column.FloatColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang3.StringUtils;

import java.sql.Date;
import java.sql.Time;
import java.util.List;

public class S3SyncConverter
        extends AbstractRowConverter<String[], RowData, String[], LogicalType> {

    private static final long serialVersionUID = -3778159110420581423L;

    public S3SyncConverter(RowType rowType, CommonConfig config) {
        super(rowType, config);
        super.commonConfig = config;
        for (LogicalType type : fieldTypes) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(createInternalConverter(type)));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(createExternalConverter(type), type));
        }
    }

    @Override
    public RowData toInternal(String[] input) {
        List<FieldConfig> fieldConfList = commonConfig.getColumn();
        ColumnRowData rowData = new ColumnRowData(fieldConfList.size());
        for (FieldConfig fieldConfig : fieldConfList) {
            StringColumn stringColumn = null;
            if (StringUtils.isBlank(fieldConfig.getValue())) {
                stringColumn = new StringColumn(input[fieldConfig.getIndex()]);
            }
            rowData.addField(assembleFieldProps(fieldConfig, stringColumn));
        }
        return rowData;
    }

    @Override
    public String[] toExternal(RowData rowData, String[] output) {
        for (int i = 0; i < output.length; i++) {
            output[i] = String.valueOf(rowData.getString(i));
        }
        return output;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case INTEGER:
                return val -> new BigDecimalColumn((Integer) val);
            case BIGINT:
                return val -> new BigDecimalColumn((Long) val);
            case FLOAT:
                return val -> new FloatColumn((Float) val);
            case DOUBLE:
                return val -> new DoubleColumn((Double) val);
            case VARCHAR:
                return val -> new StringColumn((String) val);
            case DATE:
                return val -> new SqlDateColumn((Date) val);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> new TimestampColumn((Time) val, 0);
            default:
                throw new UnsupportedTypeException(type);
        }
    }

    @Override
    protected ISerializationConverter<String[]> createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return (val, index, output) -> output[index] = null;
            case INTEGER:
                return (val, index, output) -> output[index] = String.valueOf(val.getInt(index));
            case BIGINT:
                return (val, index, output) -> output[index] = String.valueOf(val.getLong(index));
            case FLOAT:
                return (val, index, output) -> output[index] = String.valueOf(val.getFloat(index));
            case DOUBLE:
                return (val, index, output) -> output[index] = String.valueOf(val.getDouble(index));
            case VARCHAR:
                return (val, index, output) -> output[index] = val.getString(index).toString();
            case DATE:
                return (val, index, output) ->
                        output[index] =
                                ((ColumnRowData) val).getField(index).asSqlDate().toString();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (val, index, output) ->
                        output[index] = ((ColumnRowData) val).getField(index).asTime().toString();
            default:
                throw new UnsupportedTypeException(type);
        }
    }

    @Override
    protected ISerializationConverter<String[]> wrapIntoNullableExternalConverter(
            ISerializationConverter<String[]> ISerializationConverter, LogicalType type) {
        return ISerializationConverter;
    }
}

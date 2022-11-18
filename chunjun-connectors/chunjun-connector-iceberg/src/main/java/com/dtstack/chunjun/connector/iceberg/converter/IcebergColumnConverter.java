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
package com.dtstack.chunjun.connector.iceberg.converter;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;
import com.dtstack.chunjun.util.ColumnTypeUtil;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public class IcebergColumnConverter extends AbstractRowConverter<RowData, RowData, String, String> {

    private List<String> columnNameList;
    private transient Map<String, ColumnTypeUtil.DecimalInfo> decimalColInfo;

    public IcebergColumnConverter(List<FieldConfig> fieldConfList) {
        super(fieldConfList.size());
        for (int i = 0; i < fieldConfList.size(); i++) {
            String type = fieldConfList.get(i).getType();
            int left = type.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL);
            int right = type.indexOf(ConstantValue.RIGHT_PARENTHESIS_SYMBOL);
            if (left > 0 && right > 0) {
                type = type.substring(0, left);
            }
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(createInternalConverter(type)));
            //            toExternalConverters.add(
            //                    wrapIntoNullableExternalConverter(createExternalConverter(type),
            // type));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData toInternal(RowData input) throws Exception {
        ColumnRowData row = new ColumnRowData(input.getArity());
        if (input instanceof GenericRowData) {
            GenericRowData genericRowData = (GenericRowData) input;
            for (int i = 0; i < input.getArity(); i++) {
                Object field = genericRowData.getField(i);
                AbstractBaseColumn abstractBaseColumn =
                        (AbstractBaseColumn) toInternalConverters.get(i).deserialize(field);
                row.addField(abstractBaseColumn);
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
    @SuppressWarnings("unchecked")
    public String toExternal(RowData rowData, String group) throws Exception {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters.get(index).serialize(rowData, index, group);
        }
        return group;
    }

    @Override
    public RowData toInternalLookup(RowData input) {
        throw new ChunJunRuntimeException("HDFS Connector doesn't support Lookup Table Function.");
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ISerializationConverter<String> wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, String type) {
        return (rowData, index, group) -> {
            if (rowData == null || rowData.isNullAt(index)) {
                // do nothing
            } else {
                serializationConverter.serialize(rowData, index, group);
            }
        };
    }

    /** 根据json中字段的类型转成chunjun sync里的类型 ：参考 com.dtstack.chunjun.element.AbstractBaseColumn */
    @Override
    @SuppressWarnings("all")
    protected IDeserializationConverter createInternalConverter(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BOOLEAN":
                return (IDeserializationConverter<Boolean, AbstractBaseColumn>) BooleanColumn::new;
            case "TINYINT":
            case "SMALLINT":
            case "INT":
                return (IDeserializationConverter<Integer, AbstractBaseColumn>)
                        BigDecimalColumn::new;
            case "BIGINT":
            case "LONG":
                return (IDeserializationConverter<Long, AbstractBaseColumn>) BigDecimalColumn::new;
            case "FLOAT":
                return (IDeserializationConverter<Float, AbstractBaseColumn>) BigDecimalColumn::new;
            case "DOUBLE":
                return (IDeserializationConverter<Double, AbstractBaseColumn>)
                        BigDecimalColumn::new;
            case "DECIMAL":
                return (IDeserializationConverter<DecimalData, AbstractBaseColumn>)
                        val -> new BigDecimalColumn(val.toBigDecimal());
            case "STRING":
            case "VARCHAR":
            case "CHAR":
                return (IDeserializationConverter<BinaryStringData, AbstractBaseColumn>)
                        val -> new StringColumn(val.toString());
            case "TIMESTAMP":
                return (IDeserializationConverter<TimestampData, AbstractBaseColumn>)
                        val -> new TimestampColumn(val.toTimestamp());
            case "TIME":
                return (IDeserializationConverter<Integer, AbstractBaseColumn>)
                        // todo Iceberg里存储time实际是存储成一个integer类型，如63923598代表18:08:51.871
                        val -> new TimeColumn(val);
            case "DATE":
                return (IDeserializationConverter<Integer, AbstractBaseColumn>)
                        // todo Iceberg里存储date实际是存储成一个integer类型，如19235 代表2022-08-31
                        val -> new TimestampColumn(val);
            case "BINARY":
                return (IDeserializationConverter<byte[], AbstractBaseColumn>) BytesColumn::new;
            case "ARRAY":
            case "MAP":
            case "STRUCT":
            case "UNION":
            default:
                throw new UnsupportedTypeException(type);
        }
    }

    @Override
    protected ISerializationConverter<String> createExternalConverter(String type) {
        throw new ChunJunRuntimeException("Sink type conversion is not supported! ");
    }

    public void setColumnNameList(List<String> columnNameList) {
        this.columnNameList = columnNameList;
    }

    public void setDecimalColInfo(Map<String, ColumnTypeUtil.DecimalInfo> decimalColInfo) {
        this.decimalColInfo = decimalColInfo;
    }
}

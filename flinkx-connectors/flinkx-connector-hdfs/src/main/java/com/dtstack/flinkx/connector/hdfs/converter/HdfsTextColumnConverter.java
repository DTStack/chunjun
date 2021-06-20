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
package com.dtstack.flinkx.connector.hdfs.converter;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.throwable.UnsupportedTypeException;
import com.dtstack.flinkx.util.DateUtil;

import java.util.List;
import java.util.Locale;

/**
 * Date: 2021/06/16
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class HdfsTextColumnConverter extends AbstractRowConverter<RowData, RowData, List<String>, String> {

    public HdfsTextColumnConverter(List<FieldConf> fieldConfList) {
        super(fieldConfList.size());
        for (int i = 0; i < fieldConfList.size(); i++) {
            String type = fieldConfList.get(i).getType();
            int left = type.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL);
            int right = type.indexOf(ConstantValue.RIGHT_PARENTHESIS_SYMBOL);
            if (left > 0 && right > 0){
                type = type.substring(0, left);
            }
            toInternalConverters[i] = wrapIntoNullableInternalConverter(createInternalConverter(type));
            toExternalConverters[i] = wrapIntoNullableExternalConverter(createExternalConverter(type), type);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData toInternal(RowData input) {
        GenericRowData row = new GenericRowData(input.getArity());
        if(input instanceof GenericRowData){
            GenericRowData genericRowData = (GenericRowData) input;
            for (int i = 0; i < input.getArity(); i++) {
                row.setField(i, toInternalConverters[i].deserialize(genericRowData.getField(i)));
            }
        }else{
            throw new FlinkxRuntimeException("Error RowData type, RowData:[" + input + "] should be instance of GenericRowData.");
        }
        return row;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<String> toExternal(RowData rowData, List<String> list) throws Exception {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters[index].serialize(rowData, index, list);
        }
        return list;
    }

    @Override
    public RowData toInternalLookup(RowData input) {
        throw new FlinkxRuntimeException("HDFS Connector doesn't support Lookup Table Function.");
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ISerializationConverter<List<String>> wrapIntoNullableExternalConverter(ISerializationConverter serializationConverter, String type) {
        return (rowData, index, list) -> {
            if (rowData == null || rowData.isNullAt(index)) {
                list.set(index, null);
            } else {
                serializationConverter.serialize(rowData, index, list);
            }
        };
    }

    @Override
    protected IDeserializationConverter createInternalConverter(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BOOLEAN":
                return (IDeserializationConverter<String, AbstractBaseColumn>) val -> new BooleanColumn(Boolean.parseBoolean(val));
            case "TINYINT":
            case "SMALLINT":
            case "INT":
            case "BIGINT":
            case "FLOAT":
            case "DOUBLE":
            case "DECIMAL":
                return (IDeserializationConverter<String, AbstractBaseColumn>) BigDecimalColumn::new;
            case "STRING":
            case "VARCHAR":
            case "CHAR":
                return (IDeserializationConverter<String, AbstractBaseColumn>) StringColumn::new;
            case "TIMESTAMP":
            case "DATE":
                return (IDeserializationConverter<String, AbstractBaseColumn>)val -> new TimestampColumn(DateUtil.getTimestampFromStr(val));
            case "BINARY":
//                return (IDeserializationConverter<String, AbstractBaseColumn>)val -> new BytesColumn(val.getBytes(StandardCharsets.UTF_8));
            case "ARRAY":
            case "MAP":
            case "STRUCT":
            case "UNION":
            default:
                throw new UnsupportedTypeException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<List<String>> createExternalConverter(String type) {
        return (rowData, index, list) -> list.set(index, rowData.getString(index).toString());
    }
}

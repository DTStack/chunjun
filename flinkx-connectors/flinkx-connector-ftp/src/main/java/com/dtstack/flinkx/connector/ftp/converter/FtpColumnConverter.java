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

package com.dtstack.flinkx.connector.ftp.converter;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.ftp.conf.FtpConfig;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.throwable.UnsupportedTypeException;
import com.dtstack.flinkx.util.DateUtil;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * @program: flinkx
 * @author: xiuzhu
 * @create: 2021/06/19
 */
public class FtpColumnConverter extends AbstractRowConverter<RowData, RowData, String, FieldConf> {

    private final FtpConfig ftpConfig;

    public FtpColumnConverter(FtpConfig ftpConfig) {
        super(ftpConfig.getColumn().size());
        this.ftpConfig = ftpConfig;

        for (int i = 0; i < ftpConfig.getColumn().size(); i++) {
            FieldConf fieldConf = ftpConfig.getColumn().get(i);
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(createInternalConverter(fieldConf)));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(fieldConf), fieldConf));
        }
    }

    @Override
    public RowData toInternal(RowData input) throws Exception {
        GenericRowData row = new GenericRowData(input.getArity());
        if (input instanceof GenericRowData) {
            GenericRowData genericRowData = (GenericRowData) input;
            for (int i = 0; i < input.getArity(); i++) {
                row.setField(
                        i, toInternalConverters.get(i).deserialize(genericRowData.getField(i)));
            }
        } else {
            throw new FlinkxRuntimeException(
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
        for (int index = 0; index < rowData.getArity(); index++) {
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
            ISerializationConverter serializationConverter, FieldConf fieldConf) {
        return (rowData, index, list) -> {
            if (rowData == null || rowData.isNullAt(index)) {
                list.set(index, null);
            } else {
                serializationConverter.serialize(rowData, index, list);
            }
        };
    }

    @Override
    protected IDeserializationConverter createInternalConverter(FieldConf fieldConf) {
        String type = fieldConf.getType();
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BOOLEAN":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new BooleanColumn(Boolean.parseBoolean(val));
            case "TINYINT":
            case "SMALLINT":
            case "INT":
            case "BIGINT":
            case "FLOAT":
            case "DOUBLE":
            case "DECIMAL":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        BigDecimalColumn::new;
            case "STRING":
            case "VARCHAR":
            case "CHAR":
                return (IDeserializationConverter<String, AbstractBaseColumn>) StringColumn::new;
            case "TIMESTAMP":
            case "DATE":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new TimestampColumn(DateUtil.getTimestampFromStr(val));
            case "BINARY":
            case "ARRAY":
            case "MAP":
            case "STRUCT":
            case "UNION":
            default:
                throw new UnsupportedTypeException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<List<String>> createExternalConverter(FieldConf fieldConf) {
        return (rowData, index, list) ->
                list.add(index, ((ColumnRowData) rowData).getField(index).toString());
    }
}

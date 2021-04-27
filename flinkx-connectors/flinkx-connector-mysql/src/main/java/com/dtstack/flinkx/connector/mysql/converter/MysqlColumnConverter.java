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

package com.dtstack.flinkx.connector.mysql.converter;

import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.StringColumn;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.List;
import java.util.Locale;

/**
 * @author chuixue
 * @create 2021-04-27 11:46
 * @description
 */
public class MysqlColumnConverter extends MysqlRowConverter {

    public MysqlColumnConverter(List<String> typeList) {
        super.toInternalConverters = new DeserializationConverter[typeList.size()];
        super.toExternalConverters = new SerializationConverter[typeList.size()];
        for (int i = 0; i < typeList.size(); i++) {
            toInternalConverters[i] = createInternalConverter(typeList.get(i));
            toExternalConverters[i] = createExternalConverter(typeList.get(i));
        }
    }

    protected DeserializationConverter<Object> createInternalConverter(String type) {
        switch (type.toLowerCase(Locale.ENGLISH)) {
            case "int":
            case "integer":
                return val -> {
                    BigDecimal bigDecimal = new BigDecimal(val+"");
                    return new BigDecimalColumn(bigDecimal);
                };
            default:
                return val -> {
                    String string = "xxx";
                    return new StringColumn(string);
                };
        }
    }

    protected SerializationConverter<FieldNamedPreparedStatement> createExternalConverter(
            String type) {
        switch (type.toLowerCase(Locale.ENGLISH)) {
            case "int":
            case "integer":
                return (val, index, statement) -> statement.setInt(index, val.getInt(index));
            default:
                return (val, index, statement) -> statement.setObject(index, val.getString(index).toString());
        }
    }

    @Override
    public RowData toInternal(ResultSet resultSet) throws Exception {
        ColumnRowData data = new ColumnRowData(toInternalConverters.length);
        for (int i = 0; i < toInternalConverters.length; i++) {
            Object field = resultSet.getObject(i + 1);
            data.addField((AbstractBaseColumn) toInternalConverters[i].deserialize(field));
        }
        return data;
    }

    @Override
    public FieldNamedPreparedStatement toExternal(
            RowData rowData, FieldNamedPreparedStatement output) throws Exception {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters[index].serialize(rowData, index, output);
        }
        return output;
    }
}

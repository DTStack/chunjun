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

package com.dtstack.flinkx.connector.http.converter;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.http.client.DefaultRestHandler;
import com.dtstack.flinkx.connector.http.common.ConstantValue;
import com.dtstack.flinkx.connector.http.common.HttpRestConfig;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.MapUtil;

import org.apache.flink.table.data.RowData;

import org.apache.commons.collections.CollectionUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author shifang
 * @create 2021-06-07 15:51
 * @description
 */
public class HttpColumnConverter
        extends AbstractRowConverter<String, Object, Map<String, Object>, String> {

    /** restapi Conf */
    private HttpRestConfig httpRestConfig;

    public HttpColumnConverter(HttpRestConfig httpRestConfig) {
        this.httpRestConfig = httpRestConfig;

        // Only json need to extract the fields
        if (!CollectionUtils.isEmpty(httpRestConfig.getColumn())) {
            List<String> typeList =
                    httpRestConfig.getColumn().stream()
                            .map(FieldConf::getType)
                            .collect(Collectors.toList());
            this.toInternalConverters = new ArrayList<>();
            for (int i = 0; i < typeList.size(); i++) {
                toInternalConverters.add(
                        wrapIntoNullableInternalConverter(
                                createInternalConverter(typeList.get(i))));
            }
        }
    }

    @Override
    protected ISerializationConverter<Map<String, Object>> wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, String type) {
        return null;
    }

    @Override
    public RowData toInternal(String input) throws Exception {
        ColumnRowData row;
        if (httpRestConfig.getDecode().equals(ConstantValue.DEFAULT_DECODE)
                && toInternalConverters != null
                && toInternalConverters.size() > 0) {
            Map<String, Object> result =
                    DefaultRestHandler.gson.fromJson(input, GsonUtil.gsonMapTypeToken);
            row = new ColumnRowData(toInternalConverters.size());
            List<FieldConf> column = httpRestConfig.getColumn();
            for (int i = 0; i < column.size(); i++) {
                Object value =
                        MapUtil.getValueByKey(
                                result,
                                column.get(i).getName(),
                                httpRestConfig.getFieldDelimiter());
                row.addField((AbstractBaseColumn) toInternalConverters.get(i).deserialize(value));
            }
        } else {
            row = new ColumnRowData(1);
            row.addField(new StringColumn(input));
        }
        return row;
    }

    @Override
    public RowData toInternalLookup(Object input) {
        return null;
    }

    @Override
    public Map<String, Object> toExternal(RowData rowData, Map<String, Object> output)
            throws Exception {
        return null;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "INT":
            case "INTEGER":
                return val -> new BigDecimalColumn(Integer.parseInt(val.toString()));
            case "BOOLEAN":
                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
            case "TINYINT":
                return val -> new BigDecimalColumn(Byte.parseByte(val.toString()));
            case "CHAR":
            case "CHARACTER":
            case "STRING":
                return val -> new StringColumn(val.toString());
            case "SHORT":
                return val -> new BigDecimalColumn(Short.parseShort(val.toString()));
            case "LONG":
            case "BIGINT":
                return val -> new BigDecimalColumn(Long.parseLong(val.toString()));
            case "FLOAT":
                return val -> new BigDecimalColumn(Float.parseFloat(val.toString()));
            case "DOUBLE":
                return val -> new BigDecimalColumn(Double.parseDouble(val.toString()));
            case "DECIMAL":
                return val -> new BigDecimalColumn(new BigDecimal(val.toString()));
            case "DATE":
            case "TIME":
            case "DATETIME":
            case "TIMESTAMP":
                return val -> new TimestampColumn(DateUtil.getTimestampFromStr(val.toString()));
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<Map<String, Object>> createExternalConverter(String type) {
        return null;
    }
}

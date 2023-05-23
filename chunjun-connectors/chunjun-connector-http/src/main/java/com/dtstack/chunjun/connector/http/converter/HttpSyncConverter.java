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

package com.dtstack.chunjun.connector.http.converter;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.http.common.HttpRestConfig;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.DoubleColumn;
import com.dtstack.chunjun.element.column.FloatColumn;
import com.dtstack.chunjun.element.column.MapColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.util.DateUtil;

import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class HttpSyncConverter
        extends AbstractRowConverter<Map<String, Object>, Object, Map<String, Object>, String> {

    private static final long serialVersionUID = -7759259374957914968L;

    /** restapi Config */
    private final HttpRestConfig httpRestConfig;

    public HttpSyncConverter(HttpRestConfig httpRestConfig) {
        this.httpRestConfig = httpRestConfig;
        this.commonConfig = httpRestConfig;

        // Only json need to extract the fields
        if (StringUtils.isNotBlank((httpRestConfig.getFields()))) {
            String[] split = httpRestConfig.getFields().split(",");

            this.toInternalConverters = new ArrayList<>();
            for (int i = 0; i < split.length; i++) {
                toInternalConverters.add(
                        wrapIntoNullableInternalConverter(createInternalConverter("STRING")));
            }
        }
    }

    @Override
    protected ISerializationConverter<Map<String, Object>> wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, String type) {
        return null;
    }

    @Override
    public RowData toInternal(Map<String, Object> input) throws Exception {
        ColumnRowData row;

        if (toInternalConverters != null && toInternalConverters.size() > 0) {
            List<FieldConfig> fieldConfList = commonConfig.getColumn();
            // 同步任务配置了field参数(对应的类型转换都是string) 需要对每个字段进行类型转换
            row = new ColumnRowData(toInternalConverters.size());
            for (int i = 0; i < toInternalConverters.size(); i++) {
                String name = httpRestConfig.getColumn().get(i).getName();
                AbstractBaseColumn baseColumn =
                        (AbstractBaseColumn)
                                toInternalConverters.get(i).deserialize(input.get(name));

                row.addField(assembleFieldProps(fieldConfList.get(i), baseColumn));
            }
        } else {
            // 实时直接作为mapColumn
            row = new ColumnRowData(1);
            row.addField(new MapColumn(input));
        }

        return row;
    }

    @Override
    public RowData toInternalLookup(Object input) {
        return null;
    }

    @Override
    public Map<String, Object> toExternal(RowData rowData, Map<String, Object> output) {
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
                return val -> new FloatColumn(Float.parseFloat(val.toString()));
            case "DOUBLE":
                return val -> new DoubleColumn(Double.parseDouble(val.toString()));
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

/*
 *
 *  *
 *  *  * Licensed to the Apache Software Foundation (ASF) under one
 *  *  * or more contributor license agreements.  See the NOTICE file
 *  *  * distributed with this work for additional information
 *  *  * regarding copyright ownership.  The ASF licenses this file
 *  *  * to you under the Apache License, Version 2.0 (the
 *  *  * "License"); you may not use this file except in compliance
 *  *  * with the License.  You may obtain a copy of the License at
 *  *  *
 *  *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 */

package com.dtstack.flinkx.connector.influxdb.converter;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.BytesColumn;
import com.dtstack.flinkx.element.column.NullColumn;
import com.dtstack.flinkx.element.column.StringColumn;

import org.apache.commons.collections.CollectionUtils;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.lang3.StringUtils;
import org.influxdb.dto.Point;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Company：www.dtstack.com.
 *
 * @author shitou
 * @date 2022/3/8
 */
public class InfluxdbColumnConverter
        extends AbstractRowConverter<Map<String, Object>, RowData, Point.Builder, LogicalType> {

    private String format = "MSGPACK";
    private List<String> fieldNameList;
    private List<FieldConf> fieldConfList;
    private List<String> tags;
    private String timestamp;
    private TimeUnit precision;
    private String measurement;

    public InfluxdbColumnConverter(RowType rowType) {
        super(rowType);
    }

    public InfluxdbColumnConverter(
            RowType rowType,
            FlinkxCommonConf commonConf,
            List<String> fieldNameList,
            String format) {
        super(rowType, commonConf);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(fieldTypes[i]), fieldTypes[i]));
        }
        this.format = format;
        this.fieldConfList = commonConf.getColumn();
        this.fieldNameList = fieldNameList;
    }

    public InfluxdbColumnConverter(
            RowType rowType,
            FlinkxCommonConf commonConf,
            String measurement,
            List<String> fieldNameList,
            List<String> tags,
            String timestamp,
            TimeUnit precision){
        super(rowType,commonConf);
        this.measurement = measurement;
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(fieldTypes[i]), fieldTypes[i]));
        }
        this.fieldConfList = commonConf.getColumn();
        this.fieldNameList = fieldNameList;
        this.tags = tags;
        this.timestamp = timestamp;
        this.precision = precision;
    }


    @Override
    public RowData toInternal(Map<String, Object> input) throws Exception {

        if (fieldConfList.size() == 1
                && StringUtils.equals(ConstantValue.STAR_SYMBOL, fieldConfList.get(0).getName())) {
            ColumnRowData result = new ColumnRowData(fieldNameList.size());
            for (int i = 0; i < fieldNameList.size(); i++) {
                Object field = input.get(fieldNameList.get(i));
                AbstractBaseColumn baseColumn =
                        (AbstractBaseColumn) toInternalConverters.get(i).deserialize(field);
                result.addField(baseColumn);
            }
            return result;
        }

        ColumnRowData result = new ColumnRowData(fieldConfList.size());
        int converterIndex = 0;
        for (FieldConf fieldConf : fieldConfList) {
            AbstractBaseColumn baseColumn = null;
            if (StringUtils.isBlank(fieldConf.getValue())) {
                Object field = input.get(fieldConf.getName());
                baseColumn =
                        (AbstractBaseColumn)
                                toInternalConverters.get(converterIndex).deserialize(field);
                converterIndex++;
            }
            result.addField(assembleFieldProps(fieldConf, baseColumn));
        }
        return result;
    }

    @Override
    public Point.Builder toExternal(RowData rowData, Point.Builder output) throws Exception {
        Point.Builder builder = Point.measurement(measurement);
        for (FieldConf fieldConf:fieldConfList){
            
        }
        return null;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
            case INTEGER:
                return val -> {
                    if ("JSON".equals(format)) {
                        return new BigDecimalColumn(((Double) val).intValue());
                    }
                    return new BigDecimalColumn((Integer) val);
                };
            case FLOAT:
                return val -> new BigDecimalColumn(((Double) val).floatValue());
            case DOUBLE:
                return val -> new BigDecimalColumn((Double) val);
            case VARBINARY:
                return val -> new BytesColumn((byte[]) val);
            case BIGINT:
                return val -> {
                    if ("JSON".equals(format)) {
                        return new BigDecimalColumn(((Double) val).longValue());
                    }
                    if (val instanceof Long) {
                        return new BigDecimalColumn((Long) val);
                    } else {
                        return new BigDecimalColumn((Integer) val);
                    }
                };

            case VARCHAR:
                return val -> new StringColumn((String) val);
            case NULL:
                return val -> new NullColumn();
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<Point.Builder> createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()){
            case VARCHAR:
                return (val,index,builder)-> {
                    builder.addField(fieldNameList.get(index),val.getString(index).toString());
                };
            case FLOAT:
                return (val,index,builder)->{
                    builder.addField(fieldNameList.get(index),val.getFloat(index));
                };
            case INTEGER:
                return (val,index,builder)->{
                    builder.addField(fieldNameList.get(index),val.getInt(index));
                };
            case BOOLEAN:
                return (val,index,builder)->{
                    builder.addField(fieldNameList.get(index),val.getBoolean(index));
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private boolean specicalField(String fieldName,RowData value,int index,Point.Builder builder){
        if (StringUtils.isNotBlank(fieldName)){
            if(fieldName.equals(timestamp)){
                builder.time(value.getLong(index),precision);
                return true;
            }else if (CollectionUtils.isNotEmpty(tags) && tags.contains(fieldName)){
                builder.tag(fieldName,value.getRawValue(index).toString());
                return true;
            }
        }
        return false;
    }
}

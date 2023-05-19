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

package com.dtstack.chunjun.connector.kafka.converter;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.kafka.conf.KafkaConfig;
import com.dtstack.chunjun.constants.CDCConstantValue;
import com.dtstack.chunjun.converter.AbstractCDCRawTypeMapper;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.ByteColumn;
import com.dtstack.chunjun.element.column.DoubleColumn;
import com.dtstack.chunjun.element.column.FloatColumn;
import com.dtstack.chunjun.element.column.IntColumn;
import com.dtstack.chunjun.element.column.LongColumn;
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.element.column.ShortColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.util.DateUtil;

import org.apache.flink.table.data.RowData;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.dtstack.chunjun.constants.CDCConstantValue.DATABASE;
import static com.dtstack.chunjun.constants.CDCConstantValue.SCHEMA;
import static com.dtstack.chunjun.constants.CDCConstantValue.TABLE;

public class KafkaCdcSyncConverter extends AbstractCDCRawTypeMapper<RowEntity, String> {

    /** kafka Conf */
    private final KafkaConfig kafkaConfig;

    private List<String> metadata = Lists.newArrayList(DATABASE, SCHEMA, TABLE);

    public KafkaCdcSyncConverter(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    @Override
    public LinkedList<RowData> toInternal(RowEntity data) throws Exception {
        List<IDeserializationConverter> iDeserializationConverters = getConverter(data);

        Set<String> allMetaColumns = new HashSet<>();
        allMetaColumns.addAll(
                data.getMetaDataColumn().stream()
                        .map(RowEntity.MetaDataColumn::getKey)
                        .collect(Collectors.toList()));
        allMetaColumns.addAll(metadata);

        ArrayList<AbstractBaseColumn> columns = new ArrayList<>();
        ArrayList<String> headers = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(iDeserializationConverters)) {
            String key = data.getIdentifier();
            // 如果配置了schema，则以配置的schema为准，同步schema指定的字段(元数据除外)
            List<FieldConfig> fieldConfigs = kafkaConfig.getTableSchema().get(key);
            for (int i = 0; i < fieldConfigs.size(); i++) {
                FieldConfig fieldConfig = fieldConfigs.get(i);
                if (!allMetaColumns.contains(fieldConfig.getName())) {
                    if (!data.getColumnData().containsKey(fieldConfig.getName())) {
                        if (kafkaConfig.isNullReplaceNotExistsField()) {
                            columns.add(new NullColumn());
                            headers.add(fieldConfig.getName());
                        }
                    } else {
                        Object o = data.getColumnData().get(fieldConfig.getName());
                        if (o == null) {
                            columns.add(new NullColumn());
                        } else {
                            AbstractBaseColumn deserialize =
                                    (AbstractBaseColumn)
                                            iDeserializationConverters.get(i).deserialize(o);
                            AbstractBaseColumn abstractBaseColumn =
                                    assembleFieldProps(fieldConfig, deserialize);
                            columns.add(abstractBaseColumn);
                        }
                        headers.add(fieldConfig.getName());
                    }
                }
            }
        } else {
            Map<String, Object> columnData = data.getColumnData();
            columnData.forEach(
                    (k, v) -> {
                        if (!allMetaColumns.contains(k)) {
                            if (v == null) {
                                columns.add(new NullColumn());
                            } else {
                                columns.add(new StringColumn(v.toString()));
                            }
                            headers.add(k);
                        }
                    });
        }

        ColumnRowData columnRowData =
                new ColumnRowData(data.getRowKind(), allMetaColumns.size() + headers.size());

        addMetaColumn(columnRowData, data);

        columnRowData.addAllField(columns);
        columnRowData.addAllHeader(headers);

        LinkedList<RowData> rowDatas = new LinkedList<>();
        rowDatas.add(columnRowData);
        return rowDatas;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(String type) {
        // decimal(x,x)
        if (type.toUpperCase(Locale.ENGLISH).startsWith("DECIMAL")) {
            type = "DECIMAL";
        }

        // timestamp(x)
        if (type.toUpperCase(Locale.ENGLISH).startsWith("TIMESTAMP")) {
            type = "TIMESTAMP";
        }

        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "INT":
            case "INTEGER":
                return val -> new IntColumn(new BigDecimal(val.toString()).intValue());
            case "BOOLEAN":
                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
            case "TINYINT":
                return val -> new ByteColumn(Byte.parseByte(val.toString()));
            case "CHAR":
            case "CHARACTER":
            case "STRING":
            case "VARCHAR":
            case "TEXT":
                return val -> new StringColumn(val.toString());
            case "SHORT":
                return val -> new ShortColumn(new BigDecimal(val.toString()).shortValue());
            case "LONG":
            case "BIGINT":
                return val -> new LongColumn(new BigDecimal(val.toString()).longValue());
            case "FLOAT":
                return val -> new FloatColumn(new BigDecimal(val.toString()).floatValue());
            case "DOUBLE":
                return val -> new DoubleColumn(new BigDecimal(val.toString()).doubleValue());
            case "DECIMAL":
                return val -> new BigDecimalColumn(new BigDecimal(val.toString()));
            case "DATE":
                return val -> new SqlDateColumn(Date.valueOf(val.toString()));
            case "TIME":
                return val -> new TimeColumn(Time.valueOf(val.toString()));
            case "DATETIME":
                return val -> new TimestampColumn(DateUtil.getTimestampFromStr(val.toString()), 0);
            case "TIMESTAMP":
                return val -> {
                    String valStr = val.toString();
                    try {
                        return new TimestampColumn(
                                Timestamp.valueOf(valStr),
                                DateUtil.getPrecisionFromTimestampStr(valStr));
                    } catch (Exception e) {
                        return new TimestampColumn(
                                DateUtil.getTimestampFromStr(valStr),
                                DateUtil.getPrecisionFromTimestampStr(valStr));
                    }
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private void addMetaColumn(ColumnRowData columnRowData, RowEntity data) throws Exception {
        if (StringUtils.isBlank(data.getDataBase())) {
            columnRowData.addField(new NullColumn());
        } else {
            columnRowData.addField(new StringColumn(data.getDataBase()));
        }
        columnRowData.addHeader(DATABASE);
        columnRowData.addExtHeader(DATABASE);

        if (StringUtils.isBlank(data.getSchema())) {
            columnRowData.addField(new NullColumn());
        } else {
            columnRowData.addField(new StringColumn(data.getSchema()));
        }
        columnRowData.addHeader(SCHEMA);
        columnRowData.addExtHeader(SCHEMA);

        columnRowData.addField(new StringColumn(data.getTable()));
        columnRowData.addHeader(TABLE);
        columnRowData.addExtHeader(CDCConstantValue.TABLE);
        for (RowEntity.MetaDataColumn metaDataColumn : data.getMetaDataColumn()) {
            if (!metadata.contains(metaDataColumn.getKey())) {
                Object o = data.getColumnData().get(metaDataColumn.getKey());
                if (o == null) {
                    columnRowData.addField(new NullColumn());
                } else {
                    columnRowData.addField(metaDataColumn.getConvent().deserialize(o));
                }
                columnRowData.addHeader(metaDataColumn.getKey());
                columnRowData.addExtHeader(metaDataColumn.getKey());
            }
        }
    }

    private List<IDeserializationConverter> getConverter(RowEntity data) {
        String key = data.getIdentifier();
        List<IDeserializationConverter> iDeserializationConverters = cdcConverterCacheMap.get(key);
        if (CollectionUtils.isEmpty(iDeserializationConverters)) {
            Map<String, List<FieldConfig>> columnForTable = kafkaConfig.getTableSchema();
            if (MapUtils.isNotEmpty(columnForTable) && columnForTable.containsKey(key)) {
                ArrayList<IDeserializationConverter> iDeserializationConverters1 =
                        new ArrayList<>();
                for (FieldConfig fieldConfig : columnForTable.get(key)) {
                    iDeserializationConverters1.add(
                            createInternalConverter(fieldConfig.getType().getType()));
                }
                cdcConverterCacheMap.put(key, iDeserializationConverters1);
            }
        }
        return cdcConverterCacheMap.get(key);
    }
}

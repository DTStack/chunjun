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
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.kafka.conf.KafkaConfig;
import com.dtstack.chunjun.constants.CDCConstantValue;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.decoder.IDecode;
import com.dtstack.chunjun.decoder.JsonDecoder;
import com.dtstack.chunjun.decoder.TextDecoder;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.ByteColumn;
import com.dtstack.chunjun.element.column.DoubleColumn;
import com.dtstack.chunjun.element.column.FloatColumn;
import com.dtstack.chunjun.element.column.IntColumn;
import com.dtstack.chunjun.element.column.LongColumn;
import com.dtstack.chunjun.element.column.MapColumn;
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.element.column.ShortColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.util.DateUtil;
import com.dtstack.chunjun.util.MapUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.CollectionUtil;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.dtstack.chunjun.connector.kafka.option.KafkaOptions.DEFAULT_CODEC;

/**
 * @author chuixue
 * @create 2021-06-07 15:51
 * @description
 */
public class KafkaSyncConverter
        extends AbstractRowConverter<ConsumerRecord<byte[], byte[]>, Object, byte[], String> {

    /** source kafka msg decode */
    protected final IDecode decode;
    /** sink json Decoder */
    protected final JsonDecoder jsonDecoder;
    /** kafka Conf */
    protected final KafkaConfig kafkaConfig;
    /** kafka sink out fields */
    protected List<String> outList;

    public KafkaSyncConverter(KafkaConfig kafkaConfig, RowType rowType, List<String> keyTypeList) {
        super(rowType, kafkaConfig);
        this.kafkaConfig = kafkaConfig;
        this.outList = keyTypeList;
        this.jsonDecoder = new JsonDecoder(kafkaConfig.isAddMessage());
        if (DEFAULT_CODEC.defaultValue().equals(kafkaConfig.getCodec())) {
            this.decode = new JsonDecoder(kafkaConfig.isAddMessage());
        } else {
            this.decode = new TextDecoder();
        }
    }

    public KafkaSyncConverter(KafkaConfig kafkaConfig, RowType rowType) {
        super(rowType, kafkaConfig);
        this.commonConfig = this.kafkaConfig = kafkaConfig;
        this.jsonDecoder = new JsonDecoder(kafkaConfig.isAddMessage());
        if (DEFAULT_CODEC.defaultValue().equals(kafkaConfig.getCodec())) {
            this.decode = new JsonDecoder(kafkaConfig.isAddMessage());
        } else {
            this.decode = new TextDecoder();
        }

        // Only json need to extract the fields
        if (!CollectionUtils.isEmpty(kafkaConfig.getColumn())
                && DEFAULT_CODEC.defaultValue().equals(kafkaConfig.getCodec())) {
            List<TypeConfig> typeList =
                    kafkaConfig.getColumn().stream()
                            .map(FieldConfig::getType)
                            .collect(Collectors.toList());
            this.toInternalConverters = new ArrayList<>();
            for (TypeConfig s : typeList) {
                toInternalConverters.add(
                        wrapIntoNullableInternalConverter(createInternalConverter(s.getType())));
            }
        }
    }

    @Override
    public RowData toInternal(ConsumerRecord<byte[], byte[]> input) throws Exception {
        String data = new String(input.value(), StandardCharsets.UTF_8);
        Map<String, Object> map = decode.decode(data);
        ColumnRowData result;
        if (toInternalConverters == null || toInternalConverters.size() == 0) {
            if (CollectionUtils.isNotEmpty(kafkaConfig.getTopics())
                    && kafkaConfig.getTopics().size() > 1) {
                result = new ColumnRowData(4);
                result.addField(new StringColumn(input.topic()));
                result.addHeader(CDCConstantValue.TABLE);
                result.addExtHeader(CDCConstantValue.TABLE);

                result.addField(new NullColumn());
                result.addHeader(CDCConstantValue.SCHEMA);
                result.addExtHeader(CDCConstantValue.SCHEMA);

                result.addField(new NullColumn());
                result.addHeader(CDCConstantValue.DATABASE);
                result.addExtHeader(CDCConstantValue.DATABASE);

                result.addField(new MapColumn(map));
            } else {
                result = new ColumnRowData(1);
                result.addField(new MapColumn(map));
            }
        } else {
            List<FieldConfig> fieldConfList = kafkaConfig.getColumn();
            if (CollectionUtils.isNotEmpty(kafkaConfig.getTopics())
                    && kafkaConfig.getTopics().size() > 1) {
                result = new ColumnRowData(fieldConfList.size() + 3);
                result.addField(new StringColumn(input.topic()));
                result.addHeader(CDCConstantValue.TABLE);
                result.addExtHeader(CDCConstantValue.TABLE);

                result.addField(new NullColumn());
                result.addHeader(CDCConstantValue.SCHEMA);
                result.addExtHeader(CDCConstantValue.SCHEMA);

                result.addField(new NullColumn());
                result.addHeader(CDCConstantValue.DATABASE);
                result.addExtHeader(CDCConstantValue.DATABASE);
            } else {
                result = new ColumnRowData(fieldConfList.size());
            }
            for (int i = 0; i < fieldConfList.size(); i++) {
                FieldConfig fieldConf = fieldConfList.get(i);
                Object value = map.get(fieldConf.getName());
                AbstractBaseColumn baseColumn =
                        (AbstractBaseColumn) toInternalConverters.get(i).deserialize(value);
                result.addField(assembleFieldProps(fieldConf, baseColumn));
            }
        }
        return result;
    }

    @Override
    public byte[] toExternal(RowData rowData, byte[] output) throws Exception {
        Map<String, Object> map = getExternalMap(rowData);
        return MapUtil.writeValueAsString(map).getBytes(StandardCharsets.UTF_8);
    }

    protected Map<String, Object> getExternalMap(RowData rowData) {
        Map<String, Object> map;
        int arity = rowData.getArity();
        ColumnRowData row = (ColumnRowData) rowData;

        if (kafkaConfig.getTableFields() != null
                && kafkaConfig.getTableFields().size() >= arity
                && !(row.getField(0) instanceof MapColumn)) {
            map = new LinkedHashMap<>((arity << 2) / 3);
            for (int i = 0; i < arity; i++) {
                Object object = row.getField(i);
                Object value;
                if (object instanceof TimestampColumn) {
                    value = ((TimestampColumn) object).asTimestampStr();
                } else if (row.getField(i).getData() == null) {
                    value = null;
                } else {
                    value = row.getField(i).asString();
                }
                map.put(kafkaConfig.getTableFields().get(i), value);
            }

            // get partition key value
            if (!CollectionUtil.isNullOrEmpty(outList)) {
                Map<String, Object> keyPartitionMap = new LinkedHashMap<>((arity << 2) / 3);
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    if (outList.contains(entry.getKey())) {
                        keyPartitionMap.put(entry.getKey(), entry.getValue());
                    }
                }
                map = keyPartitionMap;
            }
        } else {
            String[] headers = row.getHeaders();
            if (Objects.nonNull(headers) && headers.length >= 1) {
                // cdc
                map = new HashMap<>(headers.length >> 1);
                for (String header : headers) {
                    AbstractBaseColumn val = row.getField(header);
                    if (null == val || val instanceof NullColumn) {
                        map.put(header, null);
                    } else {
                        // Timestamp需要转为yyyy-MM-dd hh:mm:ss.SSSSSS格式
                        if (val instanceof TimestampColumn) {
                            map.put(header, timeStampTostringBynacosPrecision(val.asTimestamp()));
                        } else if (val instanceof MapColumn) {
                            Object data = val.getData();
                            if (data instanceof Map) {
                                Map<String, Object> maps = (Map<String, Object>) data;
                                LinkedHashMap<String, Object> datas = new LinkedHashMap<>();
                                maps.forEach(
                                        (k, v) -> {
                                            if (v instanceof Timestamp) {
                                                datas.put(
                                                        k,
                                                        timeStampTostringBynacosPrecision(
                                                                (Timestamp) v));
                                            } else {
                                                datas.put(k, v);
                                            }
                                        });
                                map.put(header, datas);
                            } else {
                                throw new RuntimeException(
                                        "MapColumn data is not Map,map column data type is  "
                                                + data.getClass());
                            }
                        } else {
                            map.put(header, val.getData());
                        }
                    }
                }
                if (Arrays.stream(headers)
                                .filter(
                                        i ->
                                                i.equals(CDCConstantValue.BEFORE)
                                                        || i.equals(CDCConstantValue.AFTER)
                                                        || i.equals(CDCConstantValue.TABLE))
                                .collect(Collectors.toSet())
                                .size()
                        == 3) {
                    map = Collections.singletonMap("message", map);
                }
            } else if (row.getArity() == 1 && row.getField(0) instanceof MapColumn) {
                // from kafka source
                map = (Map<String, Object>) row.getField(0).getData();
            } else {
                List<String> values = new ArrayList<>(row.getArity());
                for (int i = 0; i < row.getArity(); i++) {
                    values.add(row.getField(i).asString());
                }
                map = decode.decode(String.join(",", values));
            }
        }
        return map;
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
                return val -> new IntColumn(Integer.parseInt(val.toString()));
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
                return val -> new ShortColumn(Short.parseShort(val.toString()));
            case "LONG":
            case "BIGINT":
                return val -> new LongColumn(Long.parseLong(val.toString()));
            case "FLOAT":
                return val -> new FloatColumn(Float.parseFloat(val.toString()));
            case "DOUBLE":
                return val -> new DoubleColumn(Double.parseDouble(val.toString()));
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
                        return new TimestampColumn(DateUtil.getTimestampFromStr(valStr), 0);
                    }
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    public String timeStampTostringBynacosPrecision(Timestamp t) {
        if (t.getNanos() == 0) {
            return new TimestampColumn(t, 0).asTimestampStr();
        } else {
            return t.toString();
        }
    }
}

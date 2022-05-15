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

package com.dtstack.flinkx.connector.kafka.converter;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.kafka.conf.KafkaConf;
import com.dtstack.flinkx.constants.CDCConstantValue;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.decoder.IDecode;
import com.dtstack.flinkx.decoder.JsonDecoder;
import com.dtstack.flinkx.decoder.TextDecoder;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.MapColumn;
import com.dtstack.flinkx.element.column.SqlDateColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimeColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.MapUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CollectionUtil;

import org.apache.commons.collections.CollectionUtils;

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

import static com.dtstack.flinkx.connector.kafka.option.KafkaOptions.DEFAULT_CODEC;

/**
 * @author chuixue
 * @create 2021-06-07 15:51
 * @description
 */
public class KafkaColumnConverter extends AbstractRowConverter<String, Object, byte[], String> {

    /** source kafka msg decode */
    private final IDecode decode;
    /** sink json Decoder */
    private final JsonDecoder jsonDecoder;
    /** kafka Conf */
    private final KafkaConf kafkaConf;
    /** kafka sink out fields */
    private List<String> outList;

    public KafkaColumnConverter(KafkaConf kafkaConf, List<String> keyTypeList) {
        this.kafkaConf = kafkaConf;
        this.outList = keyTypeList;
        this.jsonDecoder = new JsonDecoder();
        if (DEFAULT_CODEC.defaultValue().equals(kafkaConf.getCodec())) {
            this.decode = new JsonDecoder();
        } else {
            this.decode = new TextDecoder();
        }
    }

    public KafkaColumnConverter(KafkaConf kafkaConf) {
        this.commonConf = this.kafkaConf = kafkaConf;
        this.jsonDecoder = new JsonDecoder();
        if (DEFAULT_CODEC.defaultValue().equals(kafkaConf.getCodec())) {
            this.decode = new JsonDecoder();
        } else {
            this.decode = new TextDecoder();
        }

        // Only json need to extract the fields
        if (!CollectionUtils.isEmpty(kafkaConf.getColumn())
                && DEFAULT_CODEC.defaultValue().equals(kafkaConf.getCodec())) {
            List<String> typeList =
                    kafkaConf.getColumn().stream()
                            .map(FieldConf::getType)
                            .collect(Collectors.toList());
            this.toInternalConverters = new ArrayList<>();
            for (String s : typeList) {
                toInternalConverters.add(
                        wrapIntoNullableInternalConverter(createInternalConverter(s)));
            }
        }
    }

    @Override
    public RowData toInternal(String input) throws Exception {
        Map<String, Object> map = decode.decode(input);
        ColumnRowData result;
        if (toInternalConverters == null || toInternalConverters.size() == 0) {
            result = new ColumnRowData(1);
            result.addField(new MapColumn(map));
        } else {
            List<FieldConf> fieldConfList = kafkaConf.getColumn();
            result = new ColumnRowData(fieldConfList.size());
            for (int i = 0; i < fieldConfList.size(); i++) {
                FieldConf fieldConf = fieldConfList.get(i);
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
        Map<String, Object> map;
        int arity = rowData.getArity();
        ColumnRowData row = (ColumnRowData) rowData;

        if (kafkaConf.getTableFields() != null
                && kafkaConf.getTableFields().size() >= arity
                && !(row.getField(0) instanceof MapColumn)) {
            map = new LinkedHashMap<>((arity << 2) / 3);
            for (int i = 0; i < arity; i++) {
                Object object = row.getField(i);
                Object value;
                if (object instanceof TimestampColumn) {
                    value = ((TimestampColumn) object).asTimestampStr();
                } else {
                    value = org.apache.flink.util.StringUtils.arrayAwareToString(row.getField(i));
                }
                map.put(kafkaConf.getTableFields().get(i), value);
            }
        } else {
            String[] headers = row.getHeaders();
            if (Objects.nonNull(headers) && headers.length >= 1) {
                // cdc
                map = new HashMap<>(headers.length >> 1);
                for (String header : headers) {
                    AbstractBaseColumn val = row.getField(header);
                    if (null == val) {
                        map.put(header, null);
                    } else {
                        map.put(header, val.getData());
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

        return MapUtil.writeValueAsString(map).getBytes(StandardCharsets.UTF_8);
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
            case "VARCHAR":
            case "TEXT":
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
}

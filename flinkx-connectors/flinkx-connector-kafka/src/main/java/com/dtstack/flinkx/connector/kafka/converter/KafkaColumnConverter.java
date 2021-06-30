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

import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.kafka.conf.KafkaConf;
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
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.MapUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static com.dtstack.flinkx.connector.kafka.option.KafkaOptions.DEFAULT_CODEC;

/**
 * @author chuixue
 * @create 2021-06-07 15:51
 * @description
 */
public class KafkaColumnConverter
        extends AbstractRowConverter<String, Object, ProducerRecord, String> {

    /** source kafka msg decode */
    private IDecode decode;
    /** sink json Decoder */
    private JsonDecoder jsonDecoder;
    /** kafka Conf */
    private final KafkaConf kafkaConf;

    public KafkaColumnConverter(KafkaConf kafkaConf) {
        this.kafkaConf = kafkaConf;

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
            this.toInternalConverters = new IDeserializationConverter[typeList.size()];
            for (int i = 0; i < typeList.size(); i++) {
                toInternalConverters[i] =
                        wrapIntoNullableInternalConverter(createInternalConverter(typeList.get(i)));
            }
        }
    }

    @Override
    public RowData toInternal(String input) throws Exception {
        Map<String, Object> result = decode.decode(input);
        ColumnRowData row;
        if (toInternalConverters == null || toInternalConverters.length == 0) {
            row = new ColumnRowData(1);
            row.addField(new MapColumn(result));
        } else {
            row = new ColumnRowData(toInternalConverters.length);
            List<FieldConf> column = kafkaConf.getColumn();
            for (int i = 0; i < column.size(); i++) {
                Object value = result.get(column.get(i).getName());
                row.addField((AbstractBaseColumn) toInternalConverters[i].deserialize(value));
            }
        }
        return row;
    }

    @Override
    public ProducerRecord toExternal(RowData rowData, ProducerRecord output) throws Exception {
        Map<String, Object> map;
        int arity = rowData.getArity();
        ColumnRowData row = (ColumnRowData) rowData;

        if (kafkaConf.getTableFields() != null
                && kafkaConf.getTableFields().size() >= arity
                && !(row.getField(0) instanceof MapColumn)) {
            map = new LinkedHashMap<>((arity << 2) / 3);
            for (int i = 0; i < arity; i++) {
                map.put(
                        kafkaConf.getTableFields().get(i),
                        org.apache.flink.util.StringUtils.arrayAwareToString(row.getField(i)));
            }
        } else {
            if (arity == 1) {
                Object obj = row.getField(0);
                if (obj instanceof MapColumn) {
                    map = (Map<String, Object>) ((MapColumn) obj).getData();
                } else if (obj instanceof StringColumn) {
                    map = jsonDecoder.decode(obj.toString());
                } else {
                    map = Collections.singletonMap("message", row.toString());
                }
            } else {
                map = Collections.singletonMap("message", row.toString());
            }
        }
        byte[] bytes = MapUtil.writeValueAsString(map).getBytes(StandardCharsets.UTF_8);
        return new ProducerRecord<>(kafkaConf.getTopic(), bytes, bytes);
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
}

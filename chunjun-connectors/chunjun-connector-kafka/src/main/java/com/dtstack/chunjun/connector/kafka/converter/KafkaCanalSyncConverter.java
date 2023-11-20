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
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class KafkaCanalSyncConverter extends KafkaSyncConverter {

    private static final String FIELD_OLD = "old";
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_DATA = "data";
    private static final String OP_INSERT = "INSERT";
    private static final String OP_UPDATE = "UPDATE";
    private static final String OP_DELETE = "DELETE";
    private static final String OP_CREATE = "CREATE";

    private static final boolean IGNORE_PARSE_ERRORS = false;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /** kafka Conf */
    private final KafkaConfig kafkaConfig;

    public KafkaCanalSyncConverter(RowType rowType, KafkaConfig kafkaConfig) {
        super(rowType, kafkaConfig);
        this.commonConfig = this.kafkaConfig = kafkaConfig;
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

    @Override
    public List<RowData> toInternalList(ConsumerRecord<byte[], byte[]> input) throws Exception {
        List<RowData> rowDataList = new ArrayList<>();
        JsonNode root = OBJECT_MAPPER.readTree(input.value());
        String type = root.get(FIELD_TYPE).textValue();
        ArrayNode data = (ArrayNode) root.get(FIELD_DATA);
        try {
            if (OP_INSERT.equals(type)) {
                fillRowDataList(data, RowKind.INSERT, rowDataList);
            } else if (OP_UPDATE.equals(type)) {
                ArrayNode old = (ArrayNode) root.get(FIELD_OLD);
                fillRowDataList(old, data, rowDataList);
            } else if (OP_DELETE.equals(type)) {
                fillRowDataList(data, RowKind.DELETE, rowDataList);
            } else {
                if (!IGNORE_PARSE_ERRORS) {
                    throw new IOException(
                            format(
                                    "Unknown \"type\" value \"%s\". The Canal JSON message is '%s'",
                                    type, new String(input.value())));
                }
            }
        } catch (Throwable t) {
            // a big try catch to protect the processing.
            if (!IGNORE_PARSE_ERRORS) {
                throw new IOException(
                        format("Corrupt Canal JSON message '%s'.", new String(input.value())), t);
            }
        }
        return rowDataList;
    }

    private void fillRowDataList(ArrayNode old, ArrayNode data, List<RowData> rowDataList)
            throws Exception {
        List<RowData> updateBefore = new ArrayList<>();
        List<RowData> updateAfter = new ArrayList<>();
        fillRowDataList(data, RowKind.UPDATE_AFTER, updateAfter);
        for (int i = 0; i < old.size(); i++) {
            JsonNode item = old.get(i);
            ColumnRowData columnRowData = new ColumnRowData(old.size());
            columnRowData.setRowKind(RowKind.UPDATE_BEFORE);
            for (int j = 0; j < kafkaConfig.getColumn().size(); j++) {
                JsonNode field = item.get(kafkaConfig.getColumn().get(j).getName());
                if (Objects.isNull(field)) {
                    columnRowData.addField(((ColumnRowData) updateAfter.get(i)).getField(j));
                } else {
                    AbstractBaseColumn baseColumn =
                            (AbstractBaseColumn)
                                    toInternalConverters
                                            .get(j)
                                            .deserialize(
                                                    item.get(
                                                                    kafkaConfig
                                                                            .getColumn()
                                                                            .get(j)
                                                                            .getName())
                                                            .textValue());
                    columnRowData.addField(
                            assembleFieldProps(kafkaConfig.getColumn().get(j), baseColumn));
                }
            }
            updateBefore.add(columnRowData);
        }
        rowDataList.addAll(updateBefore);
        rowDataList.addAll(updateAfter);
    }

    private void fillRowDataList(ArrayNode data, RowKind insert, List<RowData> rowDataList)
            throws Exception {
        for (int i = 0; i < data.size(); i++) {
            JsonNode item = data.get(i);
            ColumnRowData columnRowData = new ColumnRowData(data.size());
            columnRowData.setRowKind(insert);
            for (int j = 0; j < kafkaConfig.getColumn().size(); j++) {
                AbstractBaseColumn baseColumn =
                        (AbstractBaseColumn)
                                toInternalConverters
                                        .get(j)
                                        .deserialize(
                                                item.get(kafkaConfig.getColumn().get(j).getName())
                                                        .textValue());
                columnRowData.addField(
                        assembleFieldProps(kafkaConfig.getColumn().get(j), baseColumn));
            }
            rowDataList.add(columnRowData);
        }
    }
}

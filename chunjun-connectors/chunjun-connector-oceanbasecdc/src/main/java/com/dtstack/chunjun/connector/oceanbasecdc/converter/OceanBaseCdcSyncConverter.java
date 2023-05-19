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

package com.dtstack.chunjun.connector.oceanbasecdc.converter;

import com.dtstack.chunjun.connector.oceanbasecdc.entity.OceanBaseCdcEventRow;
import com.dtstack.chunjun.constants.CDCConstantValue;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractCDCRawTypeMapper;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.MapColumn;
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;

import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import com.oceanbase.oms.logmessage.DataMessage;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.dtstack.chunjun.constants.CDCConstantValue.AFTER;
import static com.dtstack.chunjun.constants.CDCConstantValue.AFTER_;
import static com.dtstack.chunjun.constants.CDCConstantValue.BEFORE;
import static com.dtstack.chunjun.constants.CDCConstantValue.BEFORE_;
import static com.dtstack.chunjun.constants.CDCConstantValue.OP_TIME;
import static com.dtstack.chunjun.constants.CDCConstantValue.SCHEMA;
import static com.dtstack.chunjun.constants.CDCConstantValue.TABLE;
import static com.dtstack.chunjun.constants.CDCConstantValue.TS;
import static com.dtstack.chunjun.constants.CDCConstantValue.TYPE;

@SuppressWarnings({"rawtypes", "unchecked"})
public class OceanBaseCdcSyncConverter
        extends AbstractCDCRawTypeMapper<OceanBaseCdcEventRow, String> {
    private static final long serialVersionUID = 5527437395899252803L;

    /** Base column: type, schema, table, ts, opTime */
    private static final int BASE_COLUMN_SIZE = 5;

    protected final Map<String, List<String>> cdcFieldNameCacheMap = new ConcurrentHashMap<>(32);

    public OceanBaseCdcSyncConverter(boolean pavingData, boolean splitUpdate) {
        super.pavingData = pavingData;
        super.split = splitUpdate;
    }

    @Override
    public LinkedList<RowData> toInternal(OceanBaseCdcEventRow eventRow) throws Exception {
        LinkedList<RowData> result = new LinkedList<>();

        Map<String, DataMessage.Record.Field> before = eventRow.getFieldsBefore();
        Map<String, DataMessage.Record.Field> after = eventRow.getFieldsBefore();

        int columnSize =
                pavingData ? BASE_COLUMN_SIZE + before.size() + after.size() : BASE_COLUMN_SIZE + 2;

        ColumnRowData columnRowData = new ColumnRowData(columnSize);
        columnRowData.addField(new StringColumn(eventRow.getDatabase()));
        columnRowData.addHeader(SCHEMA);
        columnRowData.addField(new StringColumn(eventRow.getTable()));
        columnRowData.addHeader(TABLE);
        columnRowData.addField(new BigDecimalColumn(super.idWorker.nextId()));
        columnRowData.addHeader(TS);
        columnRowData.addField(new TimestampColumn(eventRow.getTimestamp(), 0));
        columnRowData.addHeader(OP_TIME);

        List<AbstractBaseColumn> beforeColumnList = new ArrayList<>(before.size());
        List<String> beforeHeaderList = new ArrayList<>(before.size());
        List<AbstractBaseColumn> afterColumnList = new ArrayList<>(after.size());
        List<String> afterHeaderList = new ArrayList<>(after.size());

        String key = eventRow.getDatabase() + ConstantValue.POINT_SYMBOL + eventRow.getTable();
        List<String> fieldNames = cdcFieldNameCacheMap.get(key);
        List<IDeserializationConverter> converters = super.cdcConverterCacheMap.get(key);
        if (CollectionUtils.isEmpty(fieldNames) || CollectionUtils.isEmpty(converters)) {
            Map<String, DataMessage.Record.Field> fields = before;
            if (fields.isEmpty()) {
                fields = after;
            }
            final List<String> finalFieldNames = new ArrayList<>();
            final List<IDeserializationConverter> finalConverters = new ArrayList<>();
            fields.forEach(
                    (k, v) -> {
                        finalFieldNames.add(k);
                        finalConverters.add(createInternalConverter(v.getType().name()));
                    });
            fieldNames = finalFieldNames;
            converters = finalConverters;
            cdcFieldNameCacheMap.put(key, finalFieldNames);
            cdcConverterCacheMap.put(key, finalConverters);
        }

        if (pavingData) {
            String prefixBefore = BEFORE_;
            String prefixAfter = AFTER_;
            if (split) {
                prefixBefore = "";
                prefixAfter = "";
            }
            parseColumnList(
                    fieldNames,
                    converters,
                    before,
                    beforeColumnList,
                    beforeHeaderList,
                    prefixBefore);
            parseColumnList(
                    fieldNames, converters, after, afterColumnList, afterHeaderList, prefixAfter);
        } else {
            beforeColumnList.add(new MapColumn(OceanBaseCdcEventRow.toValueMap(before)));
            beforeHeaderList.add(BEFORE);
            afterColumnList.add(new MapColumn(OceanBaseCdcEventRow.toValueMap(after)));
            afterHeaderList.add(AFTER);
        }

        if (split && DataMessage.Record.Type.UPDATE.equals(eventRow.getType())) {
            ColumnRowData copy = columnRowData.copy();
            copy.setRowKind(RowKind.UPDATE_BEFORE);
            copy.addField(new StringColumn(RowKind.UPDATE_BEFORE.name()));
            copy.addHeader(TYPE);
            copy.addExtHeader(CDCConstantValue.TYPE);
            copy.addAllField(beforeColumnList);
            copy.addAllHeader(beforeHeaderList);
            result.add(copy);

            columnRowData.setRowKind(RowKind.UPDATE_AFTER);
            columnRowData.addField(new StringColumn(RowKind.UPDATE_AFTER.name()));
            columnRowData.addHeader(TYPE);
            columnRowData.addExtHeader(CDCConstantValue.TYPE);
        } else {
            columnRowData.setRowKind(getRowKindByType(eventRow.getType().name()));
            columnRowData.addField(new StringColumn(eventRow.getType().name()));
            columnRowData.addHeader(TYPE);
            columnRowData.addExtHeader(CDCConstantValue.TYPE);
            columnRowData.addAllField(beforeColumnList);
            columnRowData.addAllHeader(beforeHeaderList);
        }
        columnRowData.addAllField(afterColumnList);
        columnRowData.addAllHeader(afterHeaderList);
        result.add(columnRowData);
        return result;
    }

    private void parseColumnList(
            List<String> fieldNames,
            List<IDeserializationConverter> converters,
            Map<String, DataMessage.Record.Field> fieldMap,
            List<AbstractBaseColumn> columnList,
            List<String> headerList,
            String after)
            throws Exception {
        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            DataMessage.Record.Field field = fieldMap.get(fieldName);
            if (field.getValue() == null) {
                columnList.add(new NullColumn());
            } else {
                columnList.add(
                        (AbstractBaseColumn)
                                converters
                                        .get(i)
                                        .deserialize(
                                                field.getValue()
                                                        .toString(StandardCharsets.UTF_8.name())));
            }
            headerList.add(after + fieldName);
        }
    }

    @Override
    protected IDeserializationConverter createInternalConverter(String type) {
        // TODO
        throw new NotImplementedException("can't convert type from log data");
    }
}

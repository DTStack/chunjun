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

package com.dtstack.chunjun.connector.kafka.serialization.ticdc;

import com.dtstack.chunjun.cdc.EventType;
import com.dtstack.chunjun.constants.CDCConstantValue;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractCDCRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.MapColumn;
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.util.DateUtil;

import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import com.google.common.collect.Lists;
import com.pingcap.ticdc.cdc.key.TicdcEventKey;
import com.pingcap.ticdc.cdc.value.TicdcEventColumn;
import com.pingcap.ticdc.cdc.value.TicdcEventRowChange;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.dtstack.chunjun.connector.kafka.serialization.ticdc.TicdcDeserializationSchema.OP_TIME;
import static com.dtstack.chunjun.connector.kafka.serialization.ticdc.TicdcDeserializationSchema.SCHEMA;
import static com.dtstack.chunjun.connector.kafka.serialization.ticdc.TicdcDeserializationSchema.TABLE;
import static com.dtstack.chunjun.connector.kafka.serialization.ticdc.TicdcDeserializationSchema.TS;
import static com.dtstack.chunjun.constants.CDCConstantValue.AFTER;
import static com.dtstack.chunjun.constants.CDCConstantValue.AFTER_;
import static com.dtstack.chunjun.constants.CDCConstantValue.BEFORE;
import static com.dtstack.chunjun.constants.CDCConstantValue.BEFORE_;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/20 星期一
 */
public class TicdcColumnConverter
        extends AbstractCDCRowConverter<Pair<TicdcEventKey, TicdcEventRowChange>, String> {

    private static final String UPDATE_OR_DELETE_TYPE = "u";

    public TicdcColumnConverter(boolean pavingData, boolean split) {
        super.pavingData = pavingData;
        super.split = split;
    }

    @Override
    public LinkedList<RowData> toInternal(Pair<TicdcEventKey, TicdcEventRowChange> input)
            throws Exception {
        LinkedList<RowData> result = Lists.newLinkedList();
        TicdcEventKey eventKey = input.getLeft();
        TicdcEventRowChange eventRowChange = input.getRight();

        List<TicdcEventColumn> beforeColumns =
                CollectionUtils.isEmpty(eventRowChange.getOldColumns())
                        ? Collections.emptyList()
                        : eventRowChange.getOldColumns();

        List<TicdcEventColumn> afterColumns =
                CollectionUtils.isEmpty(eventRowChange.getColumns())
                        ? Collections.emptyList()
                        : eventRowChange.getColumns();

        String type = getTypeFromRowChange(eventRowChange, beforeColumns);
        String schema = eventKey.getScm();
        String table = eventKey.getTbl();
        String key = schema + ConstantValue.POINT_SYMBOL + table;
        List<IDeserializationConverter> converters = super.cdcConverterCacheMap.get(key);

        // If type is delete, eventRowChange.getColumns() gets real data
        // , and eventRowChange.getOldColumns() gets empty.
        if (RowKind.DELETE.name().equals(type) && beforeColumns.isEmpty()) {
            List<TicdcEventColumn> tempColumns = Collections.emptyList();
            beforeColumns = afterColumns;
            afterColumns = tempColumns;
        }

        if (null == converters) {
            List<TicdcEventColumn> eventColumns =
                    CollectionUtils.isEmpty(beforeColumns) ? afterColumns : beforeColumns;

            converters =
                    Arrays.asList(
                            eventColumns.stream()
                                    .map(
                                            column ->
                                                    createInternalConverter(
                                                            TicdcColumnTypeHelper.findType(
                                                                    column.getT())))
                                    .toArray(IDeserializationConverter[]::new));
            cdcConverterCacheMap.put(key, converters);
        }

        // 5: schema, table, ts, opTime，type
        // 7: schema, table, ts, opTime，type, before, after
        int size = pavingData ? 5 + beforeColumns.size() + afterColumns.size() : 7;
        ColumnRowData columnRowData = new ColumnRowData(size);
        columnRowData.addField(new StringColumn(schema));
        columnRowData.addHeader(SCHEMA);
        columnRowData.addField(new StringColumn(table));
        columnRowData.addHeader(TABLE);
        columnRowData.addField(new BigDecimalColumn(super.idWorker.nextId()));
        columnRowData.addHeader(TS);
        columnRowData.addField(new TimestampColumn(eventKey.getTimestamp()));
        columnRowData.addHeader(OP_TIME);

        List<AbstractBaseColumn> beforeColumnList = new ArrayList<>(beforeColumns.size());
        List<String> beforeHeaderList = new ArrayList<>(beforeColumns.size());
        List<AbstractBaseColumn> afterColumnList = new ArrayList<>(afterColumns.size());
        List<String> afterHeaderList = new ArrayList<>(afterColumns.size());

        if (pavingData) {
            processColumnList(
                    converters, beforeColumns, beforeColumnList, beforeHeaderList, BEFORE_);
            processColumnList(converters, afterColumns, afterColumnList, afterHeaderList, AFTER_);
        } else {
            beforeColumnList.add(new MapColumn(processColumnList(beforeColumns)));
            beforeHeaderList.add(BEFORE);
            afterColumnList.add(new MapColumn(processColumnList(afterColumns)));
            afterHeaderList.add(AFTER);
        }

        if (split
                && CollectionUtils.isNotEmpty(afterColumns)
                && CollectionUtils.isNotEmpty(beforeColumns)) {
            ColumnRowData copy = columnRowData.copy();
            copy.setRowKind(RowKind.UPDATE_BEFORE);
            copy.addField(new StringColumn(RowKind.UPDATE_BEFORE.name()));
            copy.addHeader(CDCConstantValue.TYPE);
            copy.addAllField(beforeColumnList);
            copy.addAllHeader(beforeHeaderList);
            result.add(copy);

            columnRowData.setRowKind(RowKind.UPDATE_AFTER);
            columnRowData.addField(new StringColumn(RowKind.UPDATE_AFTER.name()));
            columnRowData.addHeader(CDCConstantValue.TYPE);
        } else {
            columnRowData.setRowKind(getRowKindByType(type));
            columnRowData.addField(new StringColumn(type));
            columnRowData.addHeader(CDCConstantValue.TYPE);
            columnRowData.addAllField(beforeColumnList);
            columnRowData.addAllHeader(beforeHeaderList);
        }
        columnRowData.addAllField(afterColumnList);
        columnRowData.addAllHeader(afterHeaderList);

        result.add(columnRowData);
        return result;
    }

    /**
     * 解析ticdc event-column里的数据
     *
     * @param converters ticdc converters
     * @param eventColumns ticdc columns
     * @param columnList result column list
     * @param headerList row-data headers
     * @param prefix header prefix
     * @throws Exception converters exception
     */
    private void processColumnList(
            List<IDeserializationConverter> converters,
            List<TicdcEventColumn> eventColumns,
            List<AbstractBaseColumn> columnList,
            List<String> headerList,
            String prefix)
            throws Exception {
        for (int i = 0; i < eventColumns.size(); i++) {
            TicdcEventColumn eventColumn = eventColumns.get(i);
            if (null != eventColumn.getV()) {
                AbstractBaseColumn column =
                        (AbstractBaseColumn)
                                converters.get(i).deserialize(eventColumn.getV().toString());

                columnList.add(column);
            } else {
                columnList.add(new NullColumn());
            }
            headerList.add(prefix + eventColumn.getName());
        }
    }

    /**
     * 解析ticdc event-column里的数据，转为map
     *
     * @param eventColumns ticdc event-columns
     * @return map of event-column
     */
    private Map<String, Object> processColumnList(List<TicdcEventColumn> eventColumns) {
        Map<String, Object> map = Maps.newLinkedHashMapWithExpectedSize(eventColumns.size());
        for (TicdcEventColumn eventColumn : eventColumns) {
            map.put(eventColumn.getName(), eventColumn.getV());
        }
        return map;
    }

    private String getTypeFromRowChange(
            TicdcEventRowChange rowChange, List<TicdcEventColumn> beforeColumns) {
        if (UPDATE_OR_DELETE_TYPE.equalsIgnoreCase(rowChange.getUpdateOrDelete())) {
            if (CollectionUtils.isEmpty(beforeColumns)) {
                return EventType.INSERT.name();
            } else {
                return EventType.UPDATE.name();
            }
        } else {
            return EventType.DELETE.name();
        }
    }

    @Override
    protected IDeserializationConverter createInternalConverter(String type) {
        String substring = type;
        int index = type.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL);
        if (index > 0) {
            substring = type.substring(0, index);
        }
        switch (substring.toUpperCase(Locale.ENGLISH)) {
            case "BIT":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new BooleanColumn(Boolean.parseBoolean(val));
            case "TINYINT":
            case "SMALLINT":
            case "MEDIUMINT":
            case "INT":
            case "INT24":
            case "INTEGER":
            case "FLOAT":
            case "DOUBLE":
            case "REAL":
            case "LONG":
            case "BIGINT":
            case "DECIMAL":
            case "NUMERIC":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        BigDecimalColumn::new;
            case "CHAR":
            case "VARCHAR":
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            case "ENUM":
            case "SET":
            case "JSON":
                return (IDeserializationConverter<String, AbstractBaseColumn>) StringColumn::new;
            case "DATE":
            case "TIME":
            case "TIMESTAMP":
            case "DATETIME":
            case "YEAR":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new TimestampColumn(DateUtil.getTimestampFromStr(val));
            case "TINYBLOB":
            case "BLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
            case "GEOMETRY":
            case "BINARY":
            case "VARBINARY":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new BytesColumn(val.getBytes(StandardCharsets.UTF_8));
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}

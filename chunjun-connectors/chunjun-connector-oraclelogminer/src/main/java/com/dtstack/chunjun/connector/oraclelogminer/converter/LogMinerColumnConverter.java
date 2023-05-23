/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.oraclelogminer.converter;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.oraclelogminer.entity.EventRow;
import com.dtstack.chunjun.connector.oraclelogminer.entity.EventRowData;
import com.dtstack.chunjun.connector.oraclelogminer.entity.TableMetaData;
import com.dtstack.chunjun.connector.oraclelogminer.listener.LogParser;
import com.dtstack.chunjun.constants.CDCConstantValue;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractCDCRawTypeMapper;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.MapColumn;
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.util.DateUtil;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class LogMinerColumnConverter extends AbstractCDCRawTypeMapper<EventRow, String> {

    private static final long serialVersionUID = -5819041803414698030L;

    // 存储表字段
    protected final Map<String, TableMetaData> tableMetaDataCacheMap = new ConcurrentHashMap<>(32);
    protected Connection connection;
    protected final JdbcDialect defaultDialect;

    public LogMinerColumnConverter(boolean pavingData, boolean splitUpdate) {
        super.pavingData = pavingData;
        super.split = splitUpdate;
        defaultDialect =
                new JdbcDialect() {
                    @Override
                    public String dialectName() {
                        return null;
                    }

                    @Override
                    public boolean canHandle(String url) {
                        return false;
                    }

                    @Override
                    public RawTypeMapper getRawTypeConverter() {
                        return null;
                    }

                    @Override
                    public Optional<String> defaultDriverName() {
                        return Optional.of("oracle.jdbc.OracleDriver");
                    }
                };
    }

    @Override
    public LinkedList<RowData> toInternal(EventRow eventRow) throws Exception {
        LinkedList<RowData> result = new LinkedList<>();

        String eventType = eventRow.getType();
        String schema = eventRow.getSchema();
        String table = eventRow.getTable();
        String key = schema + ConstantValue.POINT_SYMBOL + table;
        List<IDeserializationConverter> converters = super.cdcConverterCacheMap.get(key);
        List<EventRowData> beforeColumnList = eventRow.getBeforeColumnList();

        // 如果缓存为空 或者 长度变了 或者名字变了  重新更新缓存
        if (CollectionUtils.isEmpty(converters)) {
            updateCache(schema, table, key, tableMetaDataCacheMap, beforeColumnList, converters);
            converters = super.cdcConverterCacheMap.get(key);
            if (CollectionUtils.isEmpty(converters)) {
                throw new RuntimeException("get converters is null key is " + key);
            }
        }

        TableMetaData metadata = tableMetaDataCacheMap.get(key);

        int size;
        if (pavingData) {
            // 7: scn, type, database, schema, table, ts, opTime
            size = 7 + eventRow.getBeforeColumnList().size() + eventRow.getAfterColumnList().size();
        } else {
            // 9: scn, type, schema, database, table, ts, opTime, before, after, lsn
            size = 9;
        }

        ColumnRowData columnRowData = new ColumnRowData(size);
        fillColumnMetaData(columnRowData, eventRow, schema, table);

        List<EventRowData> beforeList = eventRow.getBeforeColumnList();
        List<EventRowData> afterList = eventRow.getAfterColumnList();

        List<AbstractBaseColumn> beforeFieldList = new ArrayList<>(beforeList.size());
        List<String> beforeHeaderList = new ArrayList<>(beforeList.size());
        List<AbstractBaseColumn> afterFieldList = new ArrayList<>(afterList.size());
        List<String> afterHeaderList = new ArrayList<>(afterList.size());

        if (split) {
            dealEventRowSplit(columnRowData, metadata, eventRow, result);
        } else {
            if (pavingData) {
                parseColumnList(
                        converters,
                        metadata.getFieldList(),
                        beforeList,
                        beforeFieldList,
                        beforeHeaderList,
                        CDCConstantValue.BEFORE_);
                parseColumnList(
                        converters,
                        metadata.getFieldList(),
                        afterList,
                        afterFieldList,
                        afterHeaderList,
                        CDCConstantValue.AFTER_);
            } else {
                beforeFieldList.add(new MapColumn(processColumnList(beforeList)));
                beforeHeaderList.add(CDCConstantValue.BEFORE);
                afterFieldList.add(new MapColumn(processColumnList(afterList)));
                afterHeaderList.add(CDCConstantValue.AFTER);
            }

            columnRowData.setRowKind(getRowKindByType(eventType));
            columnRowData.addField(new StringColumn(eventType));
            columnRowData.addHeader(CDCConstantValue.TYPE);
            columnRowData.addAllField(beforeFieldList);
            columnRowData.addAllHeader(beforeHeaderList);
            columnRowData.addAllField(afterFieldList);
            columnRowData.addAllHeader(afterHeaderList);

            result.add(columnRowData);
        }

        return result;
    }

    public void updateCache(
            String schema,
            String table,
            String key,
            Map<String, TableMetaData> tableMetaDataCacheMap,
            List<EventRowData> beforeColumnList,
            List<IDeserializationConverter> converters) {
        TableMetaData metadata = tableMetaDataCacheMap.get(key);
        if (Objects.isNull(converters)
                || Objects.isNull(metadata)
                || beforeColumnList.size() != converters.size()
                || !beforeColumnList.stream()
                        .map(EventRowData::getName)
                        .collect(Collectors.toCollection(HashSet::new))
                        .containsAll(metadata.getFieldList())) {
            Pair<List<String>, List<TypeConfig>> latestMetaData =
                    defaultDialect.getTableMetaData(connection, schema, table, 120, null, null);
            this.converters =
                    Arrays.asList(
                            latestMetaData.getRight().stream()
                                    .map(
                                            x ->
                                                    wrapIntoNullableInternalConverter(
                                                            createInternalConverter(x.getType())))
                                    .toArray(IDeserializationConverter[]::new));
            metadata =
                    new TableMetaData(
                            schema, table, latestMetaData.getLeft(), latestMetaData.getRight());
            super.cdcConverterCacheMap.put(key, this.converters);
            tableMetaDataCacheMap.put(key, metadata);
        }
    }

    public void dealEventRowSplit(
            ColumnRowData columnRowData,
            TableMetaData metadata,
            EventRow eventRow,
            LinkedList<RowData> result)
            throws Exception {

        String eventType = eventRow.getType();

        switch (eventType.toUpperCase()) {
            case "INSERT":
                dealOneEventRowData(
                        columnRowData,
                        metadata,
                        eventRow.getAfterColumnList(),
                        RowKind.INSERT,
                        result);
                break;
            case "UPDATE":
                dealOneEventRowData(
                        columnRowData,
                        metadata,
                        eventRow.getBeforeColumnList(),
                        RowKind.UPDATE_BEFORE,
                        result);
                dealOneEventRowData(
                        columnRowData,
                        metadata,
                        eventRow.getAfterColumnList(),
                        RowKind.UPDATE_AFTER,
                        result);
                break;
            case "DELETE":
                dealOneEventRowData(
                        columnRowData,
                        metadata,
                        eventRow.getBeforeColumnList(),
                        RowKind.DELETE,
                        result);
            default:
                log.info("not support type:" + eventType.toUpperCase());
        }
    }

    public void dealOneEventRowData(
            ColumnRowData columnRowData,
            TableMetaData metadata,
            List<EventRowData> entryColumnList,
            RowKind rowKind,
            LinkedList<RowData> result)
            throws Exception {
        ColumnRowData copy = columnRowData.copy();
        copy.setRowKind(rowKind);
        List<AbstractBaseColumn> fieldList = new ArrayList<>(entryColumnList.size());
        List<String> headerList = new ArrayList<>(entryColumnList.size());
        parseColumnList(
                converters, metadata.getFieldList(), entryColumnList, fieldList, headerList, "");
        copy.addAllField(fieldList);
        copy.addAllHeader(headerList);
        result.add(copy);
    }

    public void fillColumnMetaData(
            ColumnRowData columnRowData, EventRow eventRow, String schema, String table) {
        columnRowData.addField(new BigDecimalColumn(eventRow.getScn()));
        columnRowData.addHeader(CDCConstantValue.SCN);
        columnRowData.addExtHeader(CDCConstantValue.SCN);
        columnRowData.addField(new BigDecimalColumn(eventRow.getScn()));
        columnRowData.addHeader(CDCConstantValue.LSN);
        columnRowData.addExtHeader(CDCConstantValue.LSN);
        columnRowData.addField(new NullColumn());
        columnRowData.addHeader(CDCConstantValue.DATABASE);
        columnRowData.addExtHeader(CDCConstantValue.DATABASE);
        columnRowData.addField(new StringColumn(schema));
        columnRowData.addHeader(CDCConstantValue.SCHEMA);
        columnRowData.addExtHeader(CDCConstantValue.SCHEMA);
        columnRowData.addField(new StringColumn(table));
        columnRowData.addHeader(CDCConstantValue.TABLE);
        columnRowData.addExtHeader(CDCConstantValue.TABLE);
        columnRowData.addField(new BigDecimalColumn(eventRow.getTs()));
        columnRowData.addHeader(CDCConstantValue.TS);
        columnRowData.addExtHeader(CDCConstantValue.TS);
        columnRowData.addField(new TimestampColumn(eventRow.getOpTime()));
        columnRowData.addHeader(CDCConstantValue.OP_TIME);
        columnRowData.addExtHeader(CDCConstantValue.OP_TIME);
    }

    private void parseColumnList(
            List<IDeserializationConverter> converters,
            List<String> fieldList,
            List<EventRowData> entryColumnList,
            List<AbstractBaseColumn> columnList,
            List<String> headerList,
            String prefix)
            throws Exception {
        for (int i = 0; i < entryColumnList.size(); i++) {
            EventRowData entryColumn = entryColumnList.get(i);

            // 解析的字段顺序和metadata顺序不一致 所以先从metadata里找到字段的index  再找到对应的converters
            int index = fieldList.indexOf(entryColumn.getName());
            // 字段不一致
            if (index == -1) {
                throw new RuntimeException(
                        "The fields in the log are inconsistent with those in the current meta information，The fields in the log is "
                                + GsonUtil.GSON.toJson(entryColumnList)
                                + " ,The fields in the metadata is"
                                + GsonUtil.GSON.toJson(fieldList));
            }

            if (!entryColumn.isNull()) {
                AbstractBaseColumn column =
                        (AbstractBaseColumn)
                                converters.get(index).deserialize(entryColumn.getData());
                columnList.add(column);
            } else {
                columnList.add(new NullColumn());
            }
            headerList.add(prefix + entryColumn.getName());
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
            case "NUMBER":
            case "SMALLINT":
            case "INT":
            case "INTEGER":
            case "FLOAT":
            case "DECIMAL":
            case "NUMERIC":
            case "BINARY_FLOAT":
            case "BINARY_DOUBLE":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        BigDecimalColumn::new;
            case "CHAR":
            case "NCHAR":
            case "NVARCHAR2":
            case "ROWID":
            case "VARCHAR2":
            case "VARCHAR":
            case "LONG":
            case "RAW":
            case "LONG RAW":
            case "INTERVAL YEAR":
            case "INTERVAL DAY":
            case "BLOB":
            case "CLOB":
            case "NCLOB":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> {
                            val = LogParser.parseString(val);
                            return new StringColumn(val);
                        };
            case "DATE":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> {
                            val = LogParser.parseTime(val);
                            return new TimestampColumn(DateUtil.getTimestampFromStr(val), 0);
                        };
            case "TIMESTAMP":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> {
                            val = LogParser.parseTime(val);
                            TemporalAccessor parse = DateUtil.DATETIME_FORMATTER.parse(val);
                            LocalTime localTime = parse.query(TemporalQueries.localTime());
                            LocalDate localDate = parse.query(TemporalQueries.localDate());
                            return new TimestampColumn(
                                    Timestamp.valueOf(LocalDateTime.of(localDate, localTime)));
                        };
            case "BFILE":
            case "XMLTYPE":
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private Map<String, Object> processColumnList(List<EventRowData> eventRowDataList) {
        Map<String, Object> map = Maps.newLinkedHashMapWithExpectedSize(eventRowDataList.size());
        for (EventRowData data : eventRowDataList) {
            map.put(data.getName(), data.getData());
        }
        return map;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }
}

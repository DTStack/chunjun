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
package com.dtstack.chunjun.connector.binlog.converter;

import com.dtstack.chunjun.cdc.DdlRowData;
import com.dtstack.chunjun.cdc.DdlRowDataBuilder;
import com.dtstack.chunjun.cdc.EventType;
import com.dtstack.chunjun.connector.binlog.listener.BinlogEventRow;
import com.dtstack.chunjun.constants.CDCConstantValue;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractCDCRawTypeMapper;
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

import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import com.alibaba.otter.canal.parse.inbound.mysql.ddl.DdlResult;
import com.alibaba.otter.canal.parse.inbound.mysql.ddl.DdlResultExtend;
import com.alibaba.otter.canal.parse.inbound.mysql.ddl.DruidDdlParser;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.dtstack.chunjun.constants.CDCConstantValue.AFTER;
import static com.dtstack.chunjun.constants.CDCConstantValue.AFTER_;
import static com.dtstack.chunjun.constants.CDCConstantValue.BEFORE;
import static com.dtstack.chunjun.constants.CDCConstantValue.BEFORE_;
import static com.dtstack.chunjun.constants.CDCConstantValue.DATABASE;
import static com.dtstack.chunjun.constants.CDCConstantValue.LSN;
import static com.dtstack.chunjun.constants.CDCConstantValue.OP_TIME;
import static com.dtstack.chunjun.constants.CDCConstantValue.SCHEMA;
import static com.dtstack.chunjun.constants.CDCConstantValue.TABLE;
import static com.dtstack.chunjun.constants.CDCConstantValue.TS;
import static com.dtstack.chunjun.constants.CDCConstantValue.TYPE;

public class BinlogSyncConverter extends AbstractCDCRawTypeMapper<BinlogEventRow, String> {

    private static final long serialVersionUID = -4387050357951282347L;

    public BinlogSyncConverter(boolean pavingData, boolean splitUpdate) {
        super.pavingData = pavingData;
        super.split = splitUpdate;
    }

    @Override
    public LinkedList<RowData> toInternal(BinlogEventRow binlogEventRow) throws Exception {
        LinkedList<RowData> result = new LinkedList<>();
        CanalEntry.RowChange rowChange = binlogEventRow.getRowChange();
        String eventType = rowChange.getEventType().toString();
        String schema = binlogEventRow.getSchema();
        String table = binlogEventRow.getTable();
        String key = schema + ConstantValue.POINT_SYMBOL + table;

        if (rowChange.getIsDdl() || rowChange.getEventType().equals(CanalEntry.EventType.QUERY)) {
            super.cdcConverterCacheMap.remove(key);
            // 处理 ddl rowChange
            if (rowChange.getEventType().equals(CanalEntry.EventType.ERASE)) {
                List<DdlResult> parse =
                        DruidDdlParser.parse(
                                binlogEventRow.getRowChange().getSql(),
                                binlogEventRow.getRowChange().getDdlSchemaName());
                result.addAll(
                        parse.stream()
                                .map(
                                        i ->
                                                DdlRowDataBuilder.builder()
                                                        .setDatabaseName(i.getSchemaName())
                                                        .setTableName(i.getTableName())
                                                        .setContent(
                                                                "DROP TABLE "
                                                                        + i.getSchemaName()
                                                                        + "."
                                                                        + i.getTableName())
                                                        .setType(
                                                                binlogEventRow
                                                                        .getRowChange()
                                                                        .getEventType()
                                                                        .name())
                                                        .setLsn(
                                                                String.valueOf(
                                                                        binlogEventRow
                                                                                .getExecuteTime()))
                                                        .build())
                                .collect(Collectors.toList()));

            } else if (rowChange.getEventType().equals(CanalEntry.EventType.RENAME)) {
                List<DdlResult> parse =
                        DruidDdlParser.parse(
                                binlogEventRow.getRowChange().getSql(),
                                binlogEventRow.getRowChange().getDdlSchemaName());
                result.addAll(
                        parse.stream()
                                .map(
                                        i ->
                                                DdlRowDataBuilder.builder()
                                                        .setDatabaseName(i.getSchemaName())
                                                        .setTableName(i.getTableName())
                                                        .setContent(
                                                                "RENAME TABLE "
                                                                        + i.getOriSchemaName()
                                                                        + "."
                                                                        + i.getOriTableName()
                                                                        + " TO "
                                                                        + i.getSchemaName()
                                                                        + "."
                                                                        + i.getTableName())
                                                        .setType(
                                                                binlogEventRow
                                                                        .getRowChange()
                                                                        .getEventType()
                                                                        .name())
                                                        .setLsn(
                                                                String.valueOf(
                                                                        binlogEventRow
                                                                                .getExecuteTime()))
                                                        .build())
                                .collect(Collectors.toList()));
            } else if (rowChange.getEventType().equals(CanalEntry.EventType.QUERY)) {
                List<DdlResult> parse =
                        DruidDdlParser.parse(
                                binlogEventRow.getRowChange().getSql(),
                                binlogEventRow.getRowChange().getDdlSchemaName());
                AtomicInteger index = new AtomicInteger();
                result.addAll(
                        parse.stream()
                                .map(
                                        i -> {
                                            if (i instanceof DdlResultExtend) {
                                                if (EventType.DROP_SCHEMA.equals(
                                                        ((DdlResultExtend) i)
                                                                .getChunjunEventType())) {
                                                    return DdlRowDataBuilder.builder()
                                                            .setDatabaseName(null)
                                                            .setSchemaName(i.getSchemaName())
                                                            .setTableName(i.getTableName())
                                                            .setContent(
                                                                    "drop schema "
                                                                            + i.getSchemaName())
                                                            .setType(EventType.DROP_SCHEMA.name())
                                                            .setLsn(binlogEventRow.getLsn())
                                                            .setLsnSequence(
                                                                    index.getAndIncrement() + "")
                                                            .build();
                                                } else if (EventType.CREATE_SCHEMA.equals(
                                                        ((DdlResultExtend) i)
                                                                .getChunjunEventType())) {
                                                    return DdlRowDataBuilder.builder()
                                                            .setDatabaseName(null)
                                                            .setSchemaName(i.getSchemaName())
                                                            .setTableName(i.getTableName())
                                                            .setContent(
                                                                    "create schema "
                                                                            + i.getSchemaName())
                                                            .setType(EventType.CREATE_SCHEMA.name())
                                                            .setLsn(binlogEventRow.getLsn())
                                                            .setLsnSequence(
                                                                    index.getAndIncrement() + "")
                                                            .build();
                                                } else if (EventType.ALTER_TABLE_COMMENT.equals(
                                                        ((DdlResultExtend) i)
                                                                .getChunjunEventType())) {
                                                    return swapEventToDdlRowData(binlogEventRow);
                                                }
                                            }
                                            throw new RuntimeException(
                                                    "not support sql: "
                                                            + binlogEventRow
                                                                    .getRowChange()
                                                                    .getSql());
                                        })
                                .collect(Collectors.toList()));
            } else {
                result.add(swapEventToDdlRowData(binlogEventRow));
            }
        }

        List<IDeserializationConverter> converters = super.cdcConverterCacheMap.get(key);

        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            if (converters == null) {
                List<CanalEntry.Column> list = rowData.getBeforeColumnsList();
                if (CollectionUtils.isEmpty(list)) {
                    list = rowData.getAfterColumnsList();
                }
                converters =
                        Arrays.asList(
                                list.stream()
                                        .map(x -> createInternalConverter(x.getMysqlType()))
                                        .toArray(IDeserializationConverter[]::new));
                cdcConverterCacheMap.put(key, converters);
            }

            int size;
            if (pavingData) {
                // 5: schema, table, ts, opTime，type
                size =
                        7
                                + rowData.getAfterColumnsList().size()
                                + rowData.getBeforeColumnsList().size();
            } else {
                // 7: schema, table, ts, opTime，type, before, after
                size = 9;
            }

            ColumnRowData columnRowData = new ColumnRowData(size);
            columnRowData.addField(new NullColumn());
            columnRowData.addHeader(DATABASE);
            columnRowData.addExtHeader(DATABASE);
            columnRowData.addField(new StringColumn(schema));
            columnRowData.addHeader(SCHEMA);
            columnRowData.addExtHeader(SCHEMA);
            columnRowData.addField(new StringColumn(table));
            columnRowData.addHeader(TABLE);
            columnRowData.addExtHeader(CDCConstantValue.TABLE);
            columnRowData.addField(new BigDecimalColumn(super.idWorker.nextId()));
            columnRowData.addHeader(TS);
            columnRowData.addExtHeader(TS);
            columnRowData.addField(new StringColumn(binlogEventRow.getLsn()));
            columnRowData.addHeader(LSN);
            columnRowData.addExtHeader(LSN);
            columnRowData.addField(
                    new StringColumn(String.valueOf(binlogEventRow.getExecuteTime())));
            columnRowData.addHeader(OP_TIME);
            columnRowData.addExtHeader(CDCConstantValue.OP_TIME);

            List<CanalEntry.Column> beforeList = rowData.getBeforeColumnsList();
            List<CanalEntry.Column> afterList = rowData.getAfterColumnsList();

            List<AbstractBaseColumn> beforeColumnList = new ArrayList<>(beforeList.size());
            List<String> beforeHeaderList = new ArrayList<>(beforeList.size());
            List<AbstractBaseColumn> afterColumnList = new ArrayList<>(afterList.size());
            List<String> afterHeaderList = new ArrayList<>(afterList.size());

            if (split) {
                parseColumnList(converters, beforeList, beforeColumnList, beforeHeaderList, "");
                parseColumnList(converters, afterList, afterColumnList, afterHeaderList, "");
            } else if (pavingData) {
                parseColumnList(
                        converters, beforeList, beforeColumnList, beforeHeaderList, BEFORE_);
                parseColumnList(converters, afterList, afterColumnList, afterHeaderList, AFTER_);
            } else {
                beforeColumnList.add(
                        new MapColumn(processColumnList(rowData.getBeforeColumnsList())));
                beforeHeaderList.add(BEFORE);
                afterColumnList.add(
                        new MapColumn(processColumnList(rowData.getAfterColumnsList())));
                afterHeaderList.add(AFTER);
            }

            // update类型且要拆分
            if (split && CanalEntry.EventType.UPDATE == rowChange.getEventType()) {
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
                columnRowData.setRowKind(getRowKindByType(eventType));
                columnRowData.addField(new StringColumn(eventType));
                columnRowData.addHeader(TYPE);
                columnRowData.addExtHeader(CDCConstantValue.TYPE);
                columnRowData.addAllField(beforeColumnList);
                columnRowData.addAllHeader(beforeHeaderList);
            }
            columnRowData.addAllField(afterColumnList);
            columnRowData.addAllHeader(afterHeaderList);

            result.add(columnRowData);
        }
        return result;
    }

    /**
     * 解析CanalEntry.Column
     *
     * @param converters
     * @param entryColumnList
     * @param columnList
     * @param headerList
     * @param after
     */
    private void parseColumnList(
            List<IDeserializationConverter> converters,
            List<CanalEntry.Column> entryColumnList,
            List<AbstractBaseColumn> columnList,
            List<String> headerList,
            String after)
            throws Exception {
        for (int i = 0; i < entryColumnList.size(); i++) {
            CanalEntry.Column entryColumn = entryColumnList.get(i);
            if (!entryColumn.getIsNull()) {
                AbstractBaseColumn column =
                        (AbstractBaseColumn) converters.get(i).deserialize(entryColumn.getValue());
                columnList.add(column);
            } else {
                columnList.add(new NullColumn());
            }
            headerList.add(after + entryColumn.getName());
        }
    }

    @Override
    protected IDeserializationConverter createInternalConverter(String type) {
        String substring = type;
        // 为了支持无符号类型  如 int unsigned
        if (StringUtils.contains(substring, ConstantValue.DATA_TYPE_UNSIGNED)) {
            substring = substring.replaceAll(ConstantValue.DATA_TYPE_UNSIGNED, "").trim();
        }

        if (StringUtils.contains(substring, ConstantValue.DATA_TYPE_UNSIGNED_LOWER)) {
            substring = substring.replaceAll(ConstantValue.DATA_TYPE_UNSIGNED_LOWER, "").trim();
        }

        int index = substring.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL);
        if (index > 0) {
            substring = substring.substring(0, index);
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

    /**
     * 解析CanalEntry中的Column，获取字段名及值
     *
     * @param columnList
     * @return 字段名和值的map集合
     */
    private Map<String, Object> processColumnList(List<CanalEntry.Column> columnList) {
        Map<String, Object> map = Maps.newLinkedHashMapWithExpectedSize(columnList.size());
        for (CanalEntry.Column column : columnList) {
            map.put(column.getName(), column.getValue());
        }
        return map;
    }

    /**
     * 将 binlogEventRow {@link BinlogEventRow} 转化为 ddlRowData {@link DdlRowData}
     *
     * @param binlogEventRow binlog 事件数据
     * @return ddl row-data
     */
    private DdlRowData swapEventToDdlRowData(BinlogEventRow binlogEventRow) {
        return DdlRowDataBuilder.builder()
                .setDatabaseName(null)
                .setSchemaName(binlogEventRow.getSchema())
                .setTableName(binlogEventRow.getTable())
                .setContent(binlogEventRow.getRowChange().getSql())
                .setType(binlogEventRow.getRowChange().getEventType().name())
                .setLsn(binlogEventRow.getLsn())
                .setLsnSequence("0")
                .build();
    }
}

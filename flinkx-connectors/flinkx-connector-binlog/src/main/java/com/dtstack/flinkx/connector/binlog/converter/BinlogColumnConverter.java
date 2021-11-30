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
package com.dtstack.flinkx.connector.binlog.converter;

import com.dtstack.flinkx.cdc.DdlRowData;
import com.dtstack.flinkx.connector.binlog.listener.BinlogEventRow;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.converter.AbstractCDCRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.BytesColumn;
import com.dtstack.flinkx.element.column.MapColumn;
import com.dtstack.flinkx.element.column.NullColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;
import com.dtstack.flinkx.util.DateUtil;

import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.apache.commons.collections.CollectionUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Date: 2021/04/29 Company: www.dtstack.com
 *
 * @author tudou
 */
public class BinlogColumnConverter extends AbstractCDCRowConverter<BinlogEventRow, String> {

    public BinlogColumnConverter(boolean pavingData, boolean splitUpdate) {
        super.pavingData = pavingData;
        super.split = splitUpdate;
    }

    @Override
    @SuppressWarnings("unchecked")
    public LinkedList<RowData> toInternal(BinlogEventRow binlogEventRow) throws Exception {
        LinkedList<RowData> result = new LinkedList<>();
        CanalEntry.RowChange rowChange = binlogEventRow.getRowChange();
        String eventType = rowChange.getEventType().toString();
        String schema = binlogEventRow.getSchema();
        String table = binlogEventRow.getTable();
        String key = schema + ConstantValue.POINT_SYMBOL + table;
        List<IDeserializationConverter> converters = super.cdcConverterCacheMap.get(key);

        if (rowChange.getIsDdl()) {
            // 处理 ddl rowChange
            DdlRowData ddlRowData = swapRowChangeToDdlRowData(rowChange);
            result.add(ddlRowData);
        }

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
                        5
                                + rowData.getAfterColumnsList().size()
                                + rowData.getBeforeColumnsList().size();
            } else {
                // 7: schema, table, ts, opTime，type, before, after
                size = 7;
            }

            ColumnRowData columnRowData = new ColumnRowData(size);
            columnRowData.addField(new StringColumn(schema));
            columnRowData.addHeader(SCHEMA);
            columnRowData.addField(new StringColumn(table));
            columnRowData.addHeader(TABLE);
            columnRowData.addField(new BigDecimalColumn(super.idWorker.nextId()));
            columnRowData.addHeader(TS);
            columnRowData.addField(new TimestampColumn(binlogEventRow.getExecuteTime()));
            columnRowData.addHeader(OP_TIME);

            List<CanalEntry.Column> beforeList = rowData.getBeforeColumnsList();
            List<CanalEntry.Column> afterList = rowData.getAfterColumnsList();

            List<AbstractBaseColumn> beforeColumnList = new ArrayList<>(beforeList.size());
            List<String> beforeHeaderList = new ArrayList<>(beforeList.size());
            List<AbstractBaseColumn> afterColumnList = new ArrayList<>(afterList.size());
            List<String> afterHeaderList = new ArrayList<>(afterList.size());

            if (pavingData) {
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
                copy.addAllField(beforeColumnList);
                copy.addAllHeader(beforeHeaderList);
                result.add(copy);

                columnRowData.setRowKind(RowKind.UPDATE_AFTER);
                columnRowData.addField(new StringColumn(RowKind.UPDATE_AFTER.name()));
                columnRowData.addHeader(TYPE);
            } else {
                columnRowData.setRowKind(getRowKindByType(eventType));
                columnRowData.addField(new StringColumn(eventType));
                columnRowData.addHeader(TYPE);
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
     * 将 row change 转化为 ddlRowData
     *
     * @param rowChange row change
     * @return ddl row-data
     */
    private DdlRowData swapRowChangeToDdlRowData(CanalEntry.RowChange rowChange) {
        DdlRowData ddlRowData = new DdlRowData();
        ddlRowData.setSql(rowChange.getSql());
        ddlRowData.setType(rowChange.getEventType().name());
        ddlRowData.setTableIdentifier(rowChange.getDdlSchemaName());
        return ddlRowData;
    }
}

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
package com.dtstack.flinkx.connector.sqlservercdc.convert;

import com.dtstack.flinkx.connector.sqlservercdc.entity.ChangeTable;
import com.dtstack.flinkx.connector.sqlservercdc.entity.SqlServerCdcEnum;
import com.dtstack.flinkx.connector.sqlservercdc.entity.SqlServerCdcEventRow;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.converter.AbstractCDCRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.BytesColumn;
import com.dtstack.flinkx.element.column.MapColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;
import com.dtstack.flinkx.util.StringUtil;

import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Date: 2021/05/12 Company: www.dtstack.com
 *
 * @author shifang
 */
public class SqlServerCdcColumnConverter
        extends AbstractCDCRowConverter<SqlServerCdcEventRow, String> {

    public SqlServerCdcColumnConverter(boolean pavingData, boolean splitUpdate) {
        super.pavingData = pavingData;
        super.split = splitUpdate;
    }

    @Override
    @SuppressWarnings("unchecked")
    public LinkedList<RowData> toInternal(SqlServerCdcEventRow sqlServerCdcEventRow)
            throws Exception {
        LinkedList<RowData> result = new LinkedList<>();
        ChangeTable changeTable = sqlServerCdcEventRow.getChangeTable();
        String eventType = sqlServerCdcEventRow.getType();
        String schema = sqlServerCdcEventRow.getSchema();
        String table = sqlServerCdcEventRow.getTable();
        String key = schema + ConstantValue.POINT_SYMBOL + table;
        List<IDeserializationConverter> converters = super.cdcConverterCacheMap.get(key);

        if (converters == null) {
            List<String> columnTypes = sqlServerCdcEventRow.getColumnTypes();
            converters =
                    Arrays.asList(
                            columnTypes.stream()
                                    .map(x -> createInternalConverter(x))
                                    .toArray(IDeserializationConverter[]::new));
            cdcConverterCacheMap.put(key, converters);
        }

        int size;
        if (pavingData) {
            // 5: type, schema, table, ts, opTime
            size =
                    5
                            + sqlServerCdcEventRow.getData().length
                            + sqlServerCdcEventRow.getDataPrev().length;
        } else {
            // 7: type, schema, table, ts, opTime, before, after
            size = 7;
        }

        ColumnRowData columnRowData = new ColumnRowData(size);
        columnRowData.addField(new StringColumn(schema));
        columnRowData.addHeader(SCHEMA);
        columnRowData.addField(new StringColumn(table));
        columnRowData.addHeader(TABLE);
        columnRowData.addField(new BigDecimalColumn(super.idWorker.nextId()));
        columnRowData.addHeader(TS);
        columnRowData.addField(new TimestampColumn(sqlServerCdcEventRow.getTs()));
        columnRowData.addHeader(OP_TIME);

        Object[] data = sqlServerCdcEventRow.getData();
        Object[] dataPrev = sqlServerCdcEventRow.getDataPrev();

        List<AbstractBaseColumn> beforeColumnList = new ArrayList<>(dataPrev.length);
        List<String> beforeHeaderList = new ArrayList<>(dataPrev.length);
        List<AbstractBaseColumn> afterColumnList = new ArrayList<>(data.length);
        List<String> afterHeaderList = new ArrayList<>(data.length);

        // delete pass before value,insert pass after value,update pass both
        if (pavingData) {
            switch (sqlServerCdcEventRow.getType().toUpperCase(Locale.ENGLISH)) {
                case "DELETE":
                    parseColumnList(
                            converters,
                            dataPrev,
                            changeTable.getColumnList(),
                            beforeColumnList,
                            beforeHeaderList,
                            BEFORE_);
                    break;
                case "INSERT":
                    parseColumnList(
                            converters,
                            data,
                            changeTable.getColumnList(),
                            afterColumnList,
                            afterHeaderList,
                            AFTER_);
                    break;
                case "UPDATE":
                    parseColumnList(
                            converters,
                            dataPrev,
                            changeTable.getColumnList(),
                            beforeColumnList,
                            beforeHeaderList,
                            BEFORE_);
                    parseColumnList(
                            converters,
                            data,
                            changeTable.getColumnList(),
                            afterColumnList,
                            afterHeaderList,
                            AFTER_);
                    break;
            }
        } else {
            switch (sqlServerCdcEventRow.getType().toUpperCase(Locale.ENGLISH)) {
                case "DELETE":
                    beforeColumnList.add(
                            new MapColumn(
                                    processColumnList(
                                            changeTable.getColumnList(),
                                            sqlServerCdcEventRow.getDataPrev())));
                    beforeHeaderList.add(BEFORE);
                    break;
                case "INSERT":
                    afterColumnList.add(
                            new MapColumn(
                                    processColumnList(
                                            changeTable.getColumnList(),
                                            sqlServerCdcEventRow.getData())));
                    afterHeaderList.add(AFTER);
                    break;
                case "UPDATE":
                    beforeColumnList.add(
                            new MapColumn(
                                    processColumnList(
                                            changeTable.getColumnList(),
                                            sqlServerCdcEventRow.getDataPrev())));
                    beforeHeaderList.add(BEFORE);
                    afterColumnList.add(
                            new MapColumn(
                                    processColumnList(
                                            changeTable.getColumnList(),
                                            sqlServerCdcEventRow.getData())));
                    afterHeaderList.add(AFTER);
                    break;
            }
        }

        // update operate needs split
        if (split
                && SqlServerCdcEnum.UPDATE.name.equalsIgnoreCase(sqlServerCdcEventRow.getType())) {
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
        return result;
    }

    /**
     * parse sqlServer.Column
     *
     * @param converters
     * @param columnList
     * @param headerList
     * @param after
     */
    private void parseColumnList(
            List<IDeserializationConverter> converters,
            Object[] data,
            List<String> columnsNames,
            List<AbstractBaseColumn> columnList,
            List<String> headerList,
            String after)
            throws Exception {
        for (int i = 0; i < data.length; i++) {
            headerList.add(after + columnsNames.get(i));
            if (data[i] != null) {
                AbstractBaseColumn column =
                        (AbstractBaseColumn) converters.get(i).deserialize(data[i]);
                columnList.add(column);
            } else {
                columnList.add(null);
            }
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
                return (IDeserializationConverter<Boolean, AbstractBaseColumn>)
                        val -> new BooleanColumn(val);
            case "TINYINT":
                return (IDeserializationConverter<Short, AbstractBaseColumn>) BigDecimalColumn::new;
            case "INT":
            case "INTEGER":
                return (IDeserializationConverter<Integer, AbstractBaseColumn>)
                        BigDecimalColumn::new;
            case "FLOAT":
            case "REAL":
                return (IDeserializationConverter<Object, AbstractBaseColumn>)
                        val -> (new BigDecimalColumn(val.toString()));
            case "BIGINT":
                return (IDeserializationConverter<Long, AbstractBaseColumn>) BigDecimalColumn::new;
            case "DECIMAL":
            case "NUMERIC":
                return (IDeserializationConverter<BigDecimal, AbstractBaseColumn>)
                        BigDecimalColumn::new;
            case "CHAR":
            case "NCHAR":
            case "VARCHAR":
            case "NVARCHAR":
            case "TEXT":
                return (IDeserializationConverter<String, AbstractBaseColumn>) StringColumn::new;
            case "DATE":
            case "TIME":
                return (IDeserializationConverter<? extends Date, AbstractBaseColumn>)
                        val -> {
                            Timestamp timestamp = new Timestamp(val.getTime());
                            return new TimestampColumn(timestamp);
                        };
            case "DATETIME":
            case "DATETIME2":
            case "SMALLDATETIME":
                return (IDeserializationConverter<Timestamp, AbstractBaseColumn>)
                        TimestampColumn::new;
            case "BINARY":
            case "VARBINARY":
                return (IDeserializationConverter<String, AbstractBaseColumn>)
                        val -> new BytesColumn(val.getBytes(StandardCharsets.UTF_8));
            case "TIMESTAMP":
                return (IDeserializationConverter<byte[], AbstractBaseColumn>)
                        val -> {
                            String hexString = StringUtil.bytesToHexString(val);
                            long longValue = new BigInteger(hexString, 16).longValue();
                            return new BigDecimalColumn(longValue);
                        };
                //            case "ROWVERSION":
                //            case "UNIQUEIDENTIFIER":
                //            case "CURSOR":
                //            case "TABLE":
                //            case "SQL_VARIANT":
                //            case "XML":
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    /**
     * parse sqlServer Column，get column name and value
     *
     * @param columnNames
     * @param data
     * @return a map for column name and value
     */
    private Map<String, Object> processColumnList(List<String> columnNames, Object[] data) {
        Map<String, Object> map = Maps.newLinkedHashMapWithExpectedSize(columnNames.size());
        for (int columnIndex = 0; columnIndex < columnNames.size(); columnIndex++) {
            map.put(columnNames.get(columnIndex), data[columnIndex]);
        }
        return map;
    }
}

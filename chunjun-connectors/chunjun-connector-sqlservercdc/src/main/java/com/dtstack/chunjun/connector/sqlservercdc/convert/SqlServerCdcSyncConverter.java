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
package com.dtstack.chunjun.connector.sqlservercdc.convert;

import com.dtstack.chunjun.connector.sqlservercdc.entity.ChangeTable;
import com.dtstack.chunjun.connector.sqlservercdc.entity.SqlServerCdcEnum;
import com.dtstack.chunjun.connector.sqlservercdc.entity.SqlServerCdcEventRow;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractCDCRawTypeMapper;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.ByteColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.FloatColumn;
import com.dtstack.chunjun.element.column.IntColumn;
import com.dtstack.chunjun.element.column.LongColumn;
import com.dtstack.chunjun.element.column.MapColumn;
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.util.StringUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import com.google.common.collect.Maps;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.dtstack.chunjun.constants.CDCConstantValue.AFTER;
import static com.dtstack.chunjun.constants.CDCConstantValue.AFTER_;
import static com.dtstack.chunjun.constants.CDCConstantValue.BEFORE;
import static com.dtstack.chunjun.constants.CDCConstantValue.BEFORE_;
import static com.dtstack.chunjun.constants.CDCConstantValue.OP_TIME;
import static com.dtstack.chunjun.constants.CDCConstantValue.SCHEMA;
import static com.dtstack.chunjun.constants.CDCConstantValue.TABLE;
import static com.dtstack.chunjun.constants.CDCConstantValue.TS;
import static com.dtstack.chunjun.constants.CDCConstantValue.TYPE;

public class SqlServerCdcSyncConverter
        extends AbstractCDCRawTypeMapper<SqlServerCdcEventRow, String> {

    private static final long serialVersionUID = -8218965790709917537L;

    public SqlServerCdcSyncConverter(boolean pavingData, boolean splitUpdate) {
        super.pavingData = pavingData;
        super.split = splitUpdate;
    }

    @Override
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
                                    .map(this::createInternalConverter)
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
                columnList.add(new NullColumn());
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
                return (IDeserializationConverter<Boolean, AbstractBaseColumn>) BooleanColumn::new;
            case "TINYINT":
            case "SMALLINT":
                return (IDeserializationConverter<Short, AbstractBaseColumn>)
                        val -> new ByteColumn(val.byteValue());
            case "INT":
            case "INTEGER":
                return (IDeserializationConverter<Integer, AbstractBaseColumn>) IntColumn::new;
            case "FLOAT":
            case "REAL":
                return (IDeserializationConverter<Object, AbstractBaseColumn>)
                        val -> (new FloatColumn(Float.parseFloat(val.toString())));
            case "BIGINT":
                return (IDeserializationConverter<Long, AbstractBaseColumn>) LongColumn::new;
            case "DECIMAL":
            case "NUMERIC":
            case "MONEY":
            case "SMALLMONEY":
                return (IDeserializationConverter<BigDecimal, AbstractBaseColumn>)
                        BigDecimalColumn::new;
            case "CHAR":
            case "NCHAR":
            case "VARCHAR":
            case "NVARCHAR":
            case "TEXT":
            case "NTEXT":
            case "STRING":
            case "UNIQUEIDENTIFIER":
                return (IDeserializationConverter<String, AbstractBaseColumn>) StringColumn::new;
            case "DATE":
                return (IDeserializationConverter<Date, AbstractBaseColumn>) SqlDateColumn::new;
            case "TIME":
                return (IDeserializationConverter<? extends Time, AbstractBaseColumn>)
                        TimeColumn::new;
            case "DATETIME":
                return (IDeserializationConverter<Timestamp, AbstractBaseColumn>)
                        val -> new TimestampColumn(val, 3);
            case "DATETIME2":
                return (IDeserializationConverter<Timestamp, AbstractBaseColumn>)
                        val -> new TimestampColumn(val, 7);
            case "SMALLDATETIME":
                return (IDeserializationConverter<Timestamp, AbstractBaseColumn>)
                        val -> new TimestampColumn(val, 0);
            case "BINARY":
            case "VARBINARY":
            case "IMAGE":
                return (IDeserializationConverter<byte[], AbstractBaseColumn>) BytesColumn::new;
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

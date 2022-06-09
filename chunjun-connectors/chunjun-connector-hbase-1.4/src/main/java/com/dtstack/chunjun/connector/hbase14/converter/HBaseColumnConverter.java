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

package com.dtstack.chunjun.connector.hbase14.converter;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.hbase.FunctionParser;
import com.dtstack.chunjun.connector.hbase.FunctionTree;
import com.dtstack.chunjun.connector.hbase.conf.HBaseConf;
import com.dtstack.chunjun.connector.hbase.converter.type.BINARYSTRING;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBaseColumnConverter
        extends AbstractRowConverter<Result, RowData, Mutation, LogicalType> {

    public static final String KEY_ROW_KEY = "rowkey";

    private final HBaseConf hBaseConf;
    private final List<FieldConf> fieldList;
    private final List<String> columnNames;

    // sink
    private final boolean walFlag;
    // qualifier keys
    private final byte[][][] familyAndQualifier;
    private final String encoding;
    private final String nullMode;
    private List<Integer> rowKeyColumnIndex;
    private List<String> rowKeyColumns;
    private final Integer versionColumnIndex;
    private final String versionColumnValue;
    private final SimpleDateFormat timeSecondFormat;
    private final SimpleDateFormat timeMillisecondFormat;

    private FunctionTree functionTree;

    public HBaseColumnConverter(HBaseConf hBaseConf, RowType rowType) {
        super(rowType);
        this.hBaseConf = hBaseConf;
        this.fieldList = hBaseConf.getColumn();
        this.encoding = hBaseConf.getEncoding();
        this.nullMode = hBaseConf.getNullMode();
        this.versionColumnIndex = hBaseConf.getVersionColumnIndex();
        this.versionColumnValue = hBaseConf.getVersionColumnValue();
        this.walFlag = hBaseConf.getWalFlag();
        this.familyAndQualifier = new byte[hBaseConf.getColumn().size()][][];
        this.columnNames = new ArrayList<>(hBaseConf.getColumn().size());
        for (int i = 0; i < hBaseConf.getColumn().size(); i++) {
            String name = hBaseConf.getColumn().get(i).getName();
            columnNames.add(name);
            String[] cfAndQualifier = name.split(":");
            if (cfAndQualifier.length == 2
                    && org.apache.commons.lang.StringUtils.isNotBlank(cfAndQualifier[0])
                    && org.apache.commons.lang.StringUtils.isNotBlank(cfAndQualifier[1])) {

                byte[][] qualifierKeys = new byte[2][];
                qualifierKeys[0] = Bytes.toBytes(cfAndQualifier[0]);
                qualifierKeys[1] = Bytes.toBytes(cfAndQualifier[1]);
                familyAndQualifier[i] = qualifierKeys;
            } else if (!KEY_ROW_KEY.equals(name)) {
                throw new IllegalArgumentException(
                        "hbase 中，column 的列配置格式应该是：列族:列名. 您配置的列错误：" + name);
            }
        }

        initRowKeyConfig();

        for (int i = 0; i < hBaseConf.getColumn().size(); i++) {

            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(fieldTypes[i]), fieldTypes[i]));
        }
        this.timeSecondFormat = getSimpleDateFormat(ConstantValue.TIME_SECOND_SUFFIX);
        this.timeMillisecondFormat = getSimpleDateFormat(ConstantValue.TIME_MILLISECOND_SUFFIX);
    }

    @Override
    protected ISerializationConverter<Mutation> wrapIntoNullableExternalConverter(
            ISerializationConverter<Mutation> ISerializationConverter, LogicalType type) {
        return ((rowData, index, mutation) -> {
            if (rowData != null && !rowData.isNullAt(index)) {
                ISerializationConverter.serialize(rowData, index, mutation);
            } else {
                switch (nullMode.toUpperCase()) {
                    case "SKIP":
                        return;
                    case "EMPTY":
                        ((Put) mutation)
                                .addColumn(
                                        familyAndQualifier[index][0],
                                        familyAndQualifier[index][1],
                                        HConstants.EMPTY_BYTE_ARRAY);
                        return;
                    default:
                        throw new IllegalArgumentException("Unsupported null mode: " + nullMode);
                }
            }
        });
    }

    @Override
    public RowData toInternal(Result input) throws Exception {
        ColumnRowData result = new ColumnRowData(fieldList.size());
        for (int i = 0; i < fieldList.size(); i++) {
            AbstractBaseColumn baseColumn = null;
            if (StringUtils.isBlank(fieldList.get(i).getValue())) {
                byte[] bytes;
                if (KEY_ROW_KEY.equals(fieldList.get(i).getName())) {
                    bytes = input.getRow();
                } else {
                    bytes = input.getValue(familyAndQualifier[i][0], familyAndQualifier[i][1]);
                }
                baseColumn = (AbstractBaseColumn) toInternalConverters.get(i).deserialize(bytes);
            }
            result.addField(assembleFieldProps(fieldList.get(i), baseColumn));
        }

        return result;
    }

    @Override
    public Mutation toExternal(RowData rowData, Mutation output) throws Exception {
        byte[] rowkey = getRowkey(rowData);
        Long version = getVersion(rowData);
        Put put;
        if (version == null) {
            put = new Put(rowkey);
            if (!walFlag) {
                put.setDurability(Durability.SKIP_WAL);
            }
        } else {
            put = new Put(rowkey, version);
        }

        for (int i = 0; i < fieldList.size(); i++) {
            if (rowKeyColumnIndex.contains(i)) {
                continue;
            }
            this.toExternalConverters.get(i).serialize(rowData, i, put);
        }

        return put;
    }

    /**
     * 将外部数据库类型转换为flink内部类型
     *
     * @param type type
     * @return return
     */
    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                if (type instanceof BINARYSTRING) {
                    return val -> new StringColumn(Bytes.toStringBinary((byte[]) val));
                }
                return val -> new StringColumn(new String((byte[]) val, encoding));
            case BOOLEAN:
                return val -> {
                    // from flink
                    if (((byte[]) val).length == 1) {
                        return new BooleanColumn(((byte[]) val)[0] != 0);
                    } else {
                        return new BooleanColumn(Boolean.parseBoolean(val.toString()));
                    }
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return val ->
                        new TimestampColumn(
                                new BigDecimal(new String((byte[]) val, encoding)).longValue());
            case DECIMAL:
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case FLOAT:
            case DOUBLE:
                return val -> {
                    try {
                        return new BigDecimalColumn(Bytes.toDouble((byte[]) val));
                    } catch (Exception e) {
                        return new BigDecimalColumn(new String((byte[]) val, encoding));
                    }
                };
            case BIGINT:
                return val -> {
                    try {
                        return new BigDecimalColumn(Bytes.toLong((byte[]) val));
                    } catch (Exception e) {
                        return new BigDecimalColumn(new String((byte[]) val, encoding));
                    }
                };
            case TINYINT:
                return val -> new BigDecimalColumn(((byte[]) val)[0]);
            case SMALLINT:
                return val -> new BigDecimalColumn(Bytes.toShort(((byte[]) val)));
            case TIME_WITHOUT_TIME_ZONE:
                return val -> new TimeColumn(Bytes.toInt(((byte[]) val)));
            case BINARY:
            case VARBINARY:
                return val -> new BytesColumn(((byte[]) val));
            case DATE:
                return val -> {
                    Date date;
                    try {
                        date = new Date(Bytes.toInt(((byte[]) val)));
                    } catch (Exception e) {
                        String dateValue = Bytes.toStringBinary(((byte[]) val));
                        date = DateUtils.parseDate(dateValue);
                    }
                    return new SqlDateColumn(date.getTime());
                };

            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<Object, AbstractBaseColumn>)
                        val ->
                                new TimestampColumn(
                                        Bytes.toLong((byte[]) val),
                                        ((TimestampType) (type)).getPrecision());
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    /**
     * 将flink内部的数据类型转换为外部数据库系统类型
     *
     * @param type type
     * @return return
     */
    @Override
    protected ISerializationConverter createExternalConverter(LogicalType type) {

        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                // get the underlying UTF-8 bytes
                return (rowData, index, mutation) ->
                        ((Put) mutation)
                                .addColumn(
                                        familyAndQualifier[index][0],
                                        familyAndQualifier[index][1],
                                        ((ColumnRowData) rowData)
                                                .getField(index)
                                                .asString()
                                                .getBytes(encoding));
            case BOOLEAN:
                return (rowData, index, mutation) ->
                        ((Put) mutation)
                                .addColumn(
                                        familyAndQualifier[index][0],
                                        familyAndQualifier[index][1],
                                        Bytes.toBytes(
                                                ((ColumnRowData) rowData)
                                                        .getField(index)
                                                        .asBoolean()));
            case BINARY:
            case VARBINARY:
                return (rowData, index, mutation) ->
                        ((Put) mutation)
                                .addColumn(
                                        familyAndQualifier[index][0],
                                        familyAndQualifier[index][1],
                                        ((ColumnRowData) rowData).getField(index).asBinary());
            case DECIMAL:
                return (rowData, index, mutation) ->
                        ((Put) mutation)
                                .addColumn(
                                        familyAndQualifier[index][0],
                                        familyAndQualifier[index][1],
                                        Bytes.toBytes(
                                                ((ColumnRowData) rowData)
                                                        .getField(index)
                                                        .asBigDecimal()));
            case TINYINT:
                return (rowData, index, mutation) ->
                        ((Put) mutation)
                                .addColumn(
                                        familyAndQualifier[index][0],
                                        familyAndQualifier[index][1],
                                        new byte[] {
                                            ((ColumnRowData) rowData)
                                                    .getField(index)
                                                    .asInt()
                                                    .byteValue()
                                        });
            case SMALLINT:
                return (rowData, index, mutation) ->
                        ((Put) mutation)
                                .addColumn(
                                        familyAndQualifier[index][0],
                                        familyAndQualifier[index][1],
                                        Bytes.toBytes(
                                                ((ColumnRowData) rowData)
                                                        .getField(index)
                                                        .asShort()));
            case INTEGER:
            case DATE:
            case INTERVAL_YEAR_MONTH:
                return (rowData, index, mutation) ->
                        ((Put) mutation)
                                .addColumn(
                                        familyAndQualifier[index][0],
                                        familyAndQualifier[index][1],
                                        Bytes.toBytes(
                                                ((ColumnRowData) rowData).getField(index).asInt()));
            case TIME_WITHOUT_TIME_ZONE:
                return (rowData, index, mutation) ->
                        ((Put) mutation)
                                .addColumn(
                                        familyAndQualifier[index][0],
                                        familyAndQualifier[index][1],
                                        Bytes.toBytes(
                                                ((ColumnRowData) rowData)
                                                        .getField(index)
                                                        .asTime()
                                                        .getTime()));
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (rowData, index, mutation) ->
                        ((Put) mutation)
                                .addColumn(
                                        familyAndQualifier[index][0],
                                        familyAndQualifier[index][1],
                                        Bytes.toBytes(
                                                ((ColumnRowData) rowData)
                                                        .getField(index)
                                                        .asLong()));
            case FLOAT:
                return (rowData, index, mutation) ->
                        ((Put) mutation)
                                .addColumn(
                                        familyAndQualifier[index][0],
                                        familyAndQualifier[index][1],
                                        Bytes.toBytes(
                                                ((ColumnRowData) rowData)
                                                        .getField(index)
                                                        .asFloat()));
            case DOUBLE:
                return (rowData, index, mutation) ->
                        ((Put) mutation)
                                .addColumn(
                                        familyAndQualifier[index][0],
                                        familyAndQualifier[index][1],
                                        Bytes.toBytes(
                                                ((ColumnRowData) rowData)
                                                        .getField(index)
                                                        .asDouble()));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (rowData, index, mutation) ->
                        ((Put) mutation)
                                .addColumn(
                                        familyAndQualifier[index][0],
                                        familyAndQualifier[index][1],
                                        Bytes.toBytes(
                                                ((ColumnRowData) rowData)
                                                        .getField(index)
                                                        .asTimestamp()
                                                        .getTime()));
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private byte[] getRowkey(RowData record) throws Exception {
        Map<String, Object> nameValueMap = new HashMap<>((rowKeyColumnIndex.size() << 2) / 3);
        for (Integer keyColumnIndex : rowKeyColumnIndex) {
            nameValueMap.put(
                    columnNames.get(keyColumnIndex),
                    ((ColumnRowData) record).getField(keyColumnIndex).getData());
        }

        String rowKeyStr = functionTree.evaluate(nameValueMap);
        return rowKeyStr.getBytes(StandardCharsets.UTF_8);
    }

    public Long getVersion(RowData record) {
        if (versionColumnIndex == null && StringUtils.isBlank(versionColumnValue)) {
            return null;
        }

        Object timeStampValue = versionColumnValue;
        if (versionColumnIndex != null) {
            // 指定列作为版本,long/doubleColumn直接record.aslong, 其它类型尝试用yyyy-MM-dd HH:mm:ss,yyyy-MM-dd
            // HH:mm:ss SSS去format
            if (versionColumnIndex >= record.getArity() || versionColumnIndex < 0) {
                throw new IllegalArgumentException(
                        "version column index out of range: " + versionColumnIndex);
            }
            if (record.isNullAt(versionColumnIndex)) {
                throw new IllegalArgumentException("null verison column!");
            }

            timeStampValue = ((ColumnRowData) record).getField(versionColumnIndex).getData();
        }

        if (timeStampValue instanceof Long) {
            return (Long) timeStampValue;
        } else if (timeStampValue instanceof Double) {
            return ((Double) timeStampValue).longValue();
        } else if (timeStampValue instanceof String) {

            try {
                return Long.valueOf(timeStampValue.toString());
            } catch (Exception e) {
                // ignore
            }
            java.util.Date date;
            try {
                date = timeMillisecondFormat.parse(timeStampValue.toString());
            } catch (ParseException e) {
                try {
                    date = timeSecondFormat.parse(timeStampValue.toString());
                } catch (ParseException e1) {
                    LOG.info(
                            String.format(
                                    "您指定第[%s]列作为hbase写入版本,但在尝试用yyyy-MM-dd HH:mm:ss 和 yyyy-MM-dd HH:mm:ss SSS 去解析为Date时均出错,请检查并修改",
                                    versionColumnIndex));
                    throw new RuntimeException(e1);
                }
            }
            return date.getTime();
        } else if (timeStampValue instanceof java.util.Date) {
            return ((Date) timeStampValue).getTime();
        } else {
            throw new RuntimeException("rowkey类型不兼容: " + timeStampValue.getClass());
        }
    }

    private SimpleDateFormat getSimpleDateFormat(String sign) {
        SimpleDateFormat format;
        if (ConstantValue.TIME_SECOND_SUFFIX.equals(sign)) {
            format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        } else {
            format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
        }
        return format;
    }

    private void initRowKeyConfig() {
        if (StringUtils.isNotBlank(hBaseConf.getRowkeyExpress())) {
            this.functionTree = FunctionParser.parse(hBaseConf.getRowkeyExpress());
            this.rowKeyColumns = FunctionParser.parseRowKeyCol(hBaseConf.getRowkeyExpress());
            this.rowKeyColumnIndex = new ArrayList<>(rowKeyColumns.size());
            for (String rowKeyColumn : rowKeyColumns) {
                int index = columnNames.indexOf(rowKeyColumn);
                if (index == -1) {
                    throw new RuntimeException(
                            "Can not get row key column from columns:" + rowKeyColumn);
                }
                rowKeyColumnIndex.add(index);
            }
        }
    }
}

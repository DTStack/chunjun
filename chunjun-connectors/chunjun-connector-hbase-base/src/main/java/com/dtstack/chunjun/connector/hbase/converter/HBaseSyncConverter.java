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

package com.dtstack.chunjun.connector.hbase.converter;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.hbase.FunctionParser;
import com.dtstack.chunjun.connector.hbase.FunctionTree;
import com.dtstack.chunjun.connector.hbase.config.HBaseConfig;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.ByteColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.DoubleColumn;
import com.dtstack.chunjun.element.column.FloatColumn;
import com.dtstack.chunjun.element.column.IntColumn;
import com.dtstack.chunjun.element.column.LongColumn;
import com.dtstack.chunjun.element.column.ShortColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.dtstack.chunjun.connector.hbase.HBaseTypeUtils.MAX_TIMESTAMP_PRECISION;
import static com.dtstack.chunjun.connector.hbase.HBaseTypeUtils.MIN_TIMESTAMP_PRECISION;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

@Slf4j
public class HBaseSyncConverter
        extends AbstractRowConverter<Result, RowData, Mutation, LogicalType> {

    public static final String KEY_ROW_KEY = "rowkey";
    private static final long serialVersionUID = 4477327158644026233L;

    private FunctionTree functionTree;

    private List<Integer> rowKeyColumnIndex;

    private final Integer versionColumnIndex;

    private final String versionColumnValue;

    private final SimpleDateFormat timeSecondFormat =
            getSimpleDateFormat(ConstantValue.TIME_SECOND_SUFFIX);
    private final SimpleDateFormat timeMillisecondFormat =
            getSimpleDateFormat(ConstantValue.TIME_MILLISECOND_SUFFIX);

    private int rowKeyIndex = -1;

    private final List<String> columnNames = new ArrayList<>();

    private final List<String> columnNamesWithoutcf = new ArrayList<>();

    private final String encoding;

    private final HBaseConfig hBaseConfig;

    private final String nullMode;

    private final List<FieldConfig> fieldList;

    private final byte[][][] familyAndQualifier;

    private final byte[][][] familyAndQualifierBack;

    private final ArrayList<HashMap<String, Integer>> columnConfig;

    private final HashSet<Integer> columnConfigIndex;

    public HBaseSyncConverter(HBaseConfig hBaseConfig, RowType rowType) {
        super(rowType, hBaseConfig);
        encoding =
                StringUtils.isEmpty(hBaseConfig.getEncoding())
                        ? "utf-8"
                        : hBaseConfig.getEncoding();
        nullMode = hBaseConfig.getNullMode();
        for (int i = 0; i < hBaseConfig.getColumn().size(); i++) {
            toInternalConverters.add(
                    i,
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
            toExternalConverters.add(
                    i,
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(rowType.getTypeAt(i)), rowType.getTypeAt(i)));
        }
        this.familyAndQualifier = new byte[rowType.getFieldCount()][][];
        this.familyAndQualifierBack = new byte[rowType.getFieldCount()][][];
        this.columnConfig = new ArrayList<>(rowType.getFieldCount());
        this.columnConfigIndex = new HashSet<>(rowType.getFieldCount());
        for (int i = 0; i < hBaseConfig.getColumn().size(); i++) {
            FieldConfig fieldConfig = hBaseConfig.getColumn().get(i);
            String name = fieldConfig.getName();
            columnNames.add(name);
            String[] cfAndQualifier = name.split(":");
            if (cfAndQualifier.length == 2
                    && StringUtils.isNotBlank(cfAndQualifier[0])
                    && StringUtils.isNotBlank(cfAndQualifier[1])) {
                columnNamesWithoutcf.add(cfAndQualifier[1]);
                byte[][] qualifierKeys = new byte[2][];
                qualifierKeys[0] = Bytes.toBytes(cfAndQualifier[0]); // 列族
                qualifierKeys[1] = Bytes.toBytes(cfAndQualifier[1]); // 列名
                columnConfig.add(i, handleColumnConfig(cfAndQualifier[1]));
                familyAndQualifier[i] = qualifierKeys;
                familyAndQualifierBack[i] = Arrays.copyOf(qualifierKeys, qualifierKeys.length);
            } else if (KEY_ROW_KEY.equals(name)) {
                rowKeyIndex = i;
                columnNamesWithoutcf.add(KEY_ROW_KEY);
                columnConfig.add(i, null);
            } else if (!StringUtils.isBlank(fieldConfig.getValue())) {
                familyAndQualifier[i] = new byte[2][];
                familyAndQualifierBack[i] = new byte[2][];
                columnConfig.add(i, null);
            } else {
                throw new IllegalArgumentException(
                        "hbase 中，column 的列配置格式应该是：列族:列名. 您配置的列错误：" + name);
            }
        }
        fieldList = hBaseConfig.getColumnMetaInfos();

        this.hBaseConfig = hBaseConfig;
        initRowKeyConfig();
        this.versionColumnIndex = hBaseConfig.getVersionColumnIndex();
        this.versionColumnValue = hBaseConfig.getVersionColumnValue();
    }

    @Override
    @SuppressWarnings("unchecked")
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
    public RowData toInternalLookup(RowData input) {
        throw new ChunJunRuntimeException("Hbase Connector doesn't support Lookup Table Function.");
    }

    @Override
    public Mutation toExternal(RowData rowData, Mutation output) throws Exception {
        byte[] rowkey = getRowkey(rowData);
        Long version = getVersion(rowData);
        Put put;
        if (version == null) {
            put = new Put(rowkey);
            if (!hBaseConfig.getWalFlag()) {
                put.setDurability(Durability.SKIP_WAL);
            }
        } else {
            put = new Put(rowkey, version);
        }

        put.setTTL(
                Optional.ofNullable(hBaseConfig.getTtl())
                        .orElseGet(() -> (long) Integer.MAX_VALUE * 1000));

        for (int i = 0; i < fieldTypes.length; i++) {
            if (rowKeyIndex == i || columnConfigIndex.contains(i)) {
                continue;
            }
            if (columnConfig.get(i) != null) {
                byte[][] qualifier = familyAndQualifier[i];
                qualifier[1] =
                        fillColumnConfig(new String(qualifier[1]), columnConfig.get(i), rowData);
                familyAndQualifier[i] = qualifier;
            }
            toExternalConverters.get(i).serialize(rowData, i, put);
            if (i == rowData.getArity() - 1) {
                for (int x = 0; x < familyAndQualifierBack.length; x++) {
                    if (familyAndQualifierBack[x] == null) {
                        familyAndQualifier[x] = null;
                    } else {
                        familyAndQualifier[x] =
                                Arrays.copyOf(
                                        familyAndQualifierBack[x],
                                        familyAndQualifierBack[x].length);
                    }
                }
            }
        }
        return put;
    }

    @Override
    protected ISerializationConverter<Mutation> wrapIntoNullableExternalConverter(
            ISerializationConverter<Mutation> serializationConverter, LogicalType type) {
        return ((rowData, index, mutation) -> {
            if (rowData != null && !rowData.isNullAt(index)) {
                serializationConverter.serialize(rowData, index, mutation);
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
    protected IDeserializationConverter createInternalConverter(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case TINYINT:
                return (IDeserializationConverter<byte[], AbstractBaseColumn>)
                        bytes -> new ByteColumn(bytes[0]);
            case BOOLEAN:
                return (IDeserializationConverter<byte[], AbstractBaseColumn>)
                        bytes -> new BooleanColumn(Bytes.toBoolean(bytes));
            case INTERVAL_DAY_TIME:
            case BIGINT:
                return (IDeserializationConverter<byte[], AbstractBaseColumn>)
                        bytes -> new LongColumn(Bytes.toLong(bytes));
            case SMALLINT:
                return (IDeserializationConverter<byte[], AbstractBaseColumn>)
                        bytes -> new ShortColumn(Bytes.toShort(bytes));
            case DOUBLE:
                return (IDeserializationConverter<byte[], AbstractBaseColumn>)
                        bytes -> new DoubleColumn(Bytes.toDouble(bytes));
            case FLOAT:
                return (IDeserializationConverter<byte[], AbstractBaseColumn>)
                        bytes -> new FloatColumn(Bytes.toFloat(bytes));
            case DECIMAL:
                return (IDeserializationConverter<byte[], AbstractBaseColumn>)
                        bytes -> new BigDecimalColumn(Bytes.toBigDecimal(bytes));
            case INTERVAL_YEAR_MONTH:
            case INTEGER:
                return (IDeserializationConverter<byte[], AbstractBaseColumn>)
                        bytes -> new IntColumn(Bytes.toInt(bytes));
            case DATE:
                return (IDeserializationConverter<byte[], AbstractBaseColumn>)
                        bytes -> {
                            Date date;
                            try {
                                date = new Date(Bytes.toInt((bytes)));
                            } catch (Exception e) {
                                String dateValue = Bytes.toStringBinary((bytes));
                                date = DateUtils.parseDate(dateValue);
                            }
                            return new SqlDateColumn(date.getTime());
                        };
            case CHAR:
            case VARCHAR:
                return (IDeserializationConverter<byte[], AbstractBaseColumn>)
                        bytes -> new StringColumn(new String(bytes, encoding));
            case BINARY:
            case VARBINARY:
                return (IDeserializationConverter<byte[], AbstractBaseColumn>) BytesColumn::new;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (IDeserializationConverter<byte[], AbstractBaseColumn>)
                        bytes -> {
                            final int timestampPrecision = getPrecision(logicalType);
                            if (timestampPrecision < MIN_TIMESTAMP_PRECISION
                                    || timestampPrecision > MAX_TIMESTAMP_PRECISION) {
                                throw new UnsupportedOperationException(
                                        String.format(
                                                "The precision %s of TIMESTAMP type is out of the range [%s, %s] supported by "
                                                        + "HBase connector",
                                                timestampPrecision,
                                                MIN_TIMESTAMP_PRECISION,
                                                MAX_TIMESTAMP_PRECISION));
                            }
                            long value = Bytes.toLong(bytes);
                            Timestamp timestamp = new Timestamp(value);
                            return new TimestampColumn(timestamp, timestampPrecision);
                        };
            case TIME_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<byte[], AbstractBaseColumn>)
                        bytes -> {
                            int value = Bytes.toInt(bytes);
                            LocalTime localTime = LocalTime.ofNanoOfDay(value * 1_000_000L);
                            Time time = Time.valueOf(localTime);
                            return new TimeColumn(time);
                        };
            default:
                throw new UnsupportedTypeException(logicalType.getTypeRoot());
        }
    }

    @Override
    protected ISerializationConverter<Mutation> createExternalConverter(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case TINYINT:
                return (rowData, pos, output) -> {
                    ColumnRowData columnRowData = (ColumnRowData) rowData;
                    AbstractBaseColumn baseColumn = columnRowData.getField(pos);
                    byte value = baseColumn.asInt().byteValue();
                    byte[] bytes = new byte[] {value};
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            case BOOLEAN:
                return (rowData, pos, output) -> {
                    ColumnRowData columnRowData = (ColumnRowData) rowData;
                    AbstractBaseColumn baseColumn = columnRowData.getField(pos);
                    Boolean value = baseColumn.asBoolean();
                    byte[] bytes = Bytes.toBytes(value);
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            case INTERVAL_DAY_TIME:
            case BIGINT:
                return (rowData, pos, output) -> {
                    ColumnRowData columnRowData = (ColumnRowData) rowData;
                    AbstractBaseColumn baseColumn = columnRowData.getField(pos);
                    Long value = baseColumn.asLong();
                    byte[] bytes = Bytes.toBytes(value);
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            case SMALLINT:
                return (rowData, pos, output) -> {
                    ColumnRowData columnRowData = (ColumnRowData) rowData;
                    AbstractBaseColumn baseColumn = columnRowData.getField(pos);
                    Short value = baseColumn.asShort();
                    byte[] bytes = Bytes.toBytes(value);
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            case DOUBLE:
                return (rowData, pos, output) -> {
                    ColumnRowData columnRowData = (ColumnRowData) rowData;
                    AbstractBaseColumn baseColumn = columnRowData.getField(pos);
                    Double value = baseColumn.asDouble();
                    byte[] bytes = Bytes.toBytes(value);
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            case FLOAT:
                return (rowData, pos, output) -> {
                    ColumnRowData columnRowData = (ColumnRowData) rowData;
                    AbstractBaseColumn baseColumn = columnRowData.getField(pos);
                    Float value = baseColumn.asFloat();
                    byte[] bytes = Bytes.toBytes(value);
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            case DECIMAL:
                return (rowData, pos, output) -> {
                    ColumnRowData columnRowData = (ColumnRowData) rowData;
                    AbstractBaseColumn baseColumn = columnRowData.getField(pos);
                    BigDecimal value = baseColumn.asBigDecimal();
                    byte[] bytes = Bytes.toBytes(value);
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            case DATE:
                return (rowData, pos, output) -> {
                    String value = ((ColumnRowData) rowData).getField(pos).asSqlDate().toString();
                    byte[] bytes = Bytes.toBytes(value);
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            case INTERVAL_YEAR_MONTH:
            case INTEGER:
                return (rowData, pos, output) -> {
                    ColumnRowData columnRowData = (ColumnRowData) rowData;
                    AbstractBaseColumn baseColumn = columnRowData.getField(pos);
                    Integer value = baseColumn.asInt();
                    byte[] bytes = Bytes.toBytes(value);
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            case CHAR:
            case VARCHAR:
                return (rowData, pos, output) -> {
                    ColumnRowData columnRowData = (ColumnRowData) rowData;
                    AbstractBaseColumn baseColumn = columnRowData.getField(pos);
                    String value = baseColumn.asString();
                    byte[] bytes = value.getBytes(Charset.forName(encoding));
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            case BINARY:
            case VARBINARY:
                return (rowData, pos, output) -> {
                    ColumnRowData columnRowData = (ColumnRowData) rowData;
                    AbstractBaseColumn baseColumn = columnRowData.getField(pos);
                    byte[] bytes = baseColumn.asBinary();
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (rowData, pos, output) -> {
                    final int timestampPrecision = getPrecision(logicalType);
                    if (timestampPrecision < MIN_TIMESTAMP_PRECISION
                            || timestampPrecision > MAX_TIMESTAMP_PRECISION) {
                        throw new UnsupportedOperationException(
                                String.format(
                                        "The precision %s of TIMESTAMP type is out of the range [%s, %s] supported by "
                                                + "HBase connector",
                                        timestampPrecision,
                                        MIN_TIMESTAMP_PRECISION,
                                        MAX_TIMESTAMP_PRECISION));
                    }
                    ColumnRowData columnRowData = (ColumnRowData) rowData;
                    AbstractBaseColumn baseColumn = columnRowData.getField(pos);
                    Timestamp timestamp = baseColumn.asTimestamp();
                    byte[] bytes = Bytes.toBytes(timestamp.getTime());
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            case TIME_WITHOUT_TIME_ZONE:
                return (rowData, pos, output) -> {
                    ColumnRowData columnRowData = (ColumnRowData) rowData;
                    AbstractBaseColumn baseColumn = columnRowData.getField(pos);
                    Time time = baseColumn.asTime();
                    int data = (int) (time.toLocalTime().toNanoOfDay() / 1_000_000L);
                    byte[] bytes = Bytes.toBytes(data);
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            default:
                throw new UnsupportedTypeException(logicalType.getTypeRoot());
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

    private void initRowKeyConfig() {
        if (StringUtils.isNotBlank(hBaseConfig.getRowkeyExpress())) {
            this.functionTree = FunctionParser.parse(hBaseConfig.getRowkeyExpress());
            List<String> rowKeyColumns =
                    FunctionParser.parseRowKeyCol(hBaseConfig.getRowkeyExpress());
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
                throw new IllegalArgumentException("null version column!");
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
            Date date;
            try {
                date = timeMillisecondFormat.parse(timeStampValue.toString());
            } catch (ParseException e) {
                try {
                    date = timeSecondFormat.parse(timeStampValue.toString());
                } catch (ParseException e1) {
                    log.info(
                            String.format(
                                    "您指定第[%s]列作为hbase写入版本,但在尝试用yyyy-MM-dd HH:mm:ss 和 yyyy-MM-dd HH:mm:ss SSS 去解析为Date时均出错,请检查并修改",
                                    versionColumnIndex));
                    throw new RuntimeException(e1);
                }
            }
            return date.getTime();
        } else if (timeStampValue instanceof Date) {
            return ((Date) timeStampValue).getTime();
        } else if (timeStampValue instanceof BigDecimal) {
            return ((BigDecimal) timeStampValue).longValue();
        } else {
            throw new RuntimeException("version 类型不兼容: " + timeStampValue.getClass());
        }
    }

    private static SimpleDateFormat getSimpleDateFormat(String sign) {
        SimpleDateFormat format;
        if (ConstantValue.TIME_SECOND_SUFFIX.equals(sign)) {
            format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        } else {
            format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
        }
        return format;
    }

    private HashMap<String, Integer> handleColumnConfig(String qualifier) {
        HashMap<String, Integer> columnConfigMap = new HashMap<>(columnNames.size());
        List<String> regexColumnNameList = FunctionParser.getRegexColumnName(qualifier);
        if (!regexColumnNameList.isEmpty()) {
            for (int i = 0; i < regexColumnNameList.size(); i++) {
                columnConfigMap.put(
                        regexColumnNameList.get(i),
                        columnNamesWithoutcf.indexOf(regexColumnNameList.get(i)));
                columnConfigIndex.add(columnNamesWithoutcf.indexOf(regexColumnNameList.get(i)));
            }
        } else {
            columnConfigMap = null;
        }
        return columnConfigMap;
    }

    private byte[] fillColumnConfig(
            String columnValue, HashMap<String, Integer> columnConfigMap, RowData rowData) {
        List<String> regexColumnNameList = FunctionParser.getRegexColumnName(columnValue);
        for (String regrexColumn : regexColumnNameList) {
            Integer columnIndex = columnConfigMap.get(regrexColumn);
            columnValue =
                    StringUtils.replace(
                            columnValue,
                            "$(" + regrexColumn + ")",
                            rowData.getString(columnIndex).toString());
        }
        return Bytes.toBytes(columnValue);
    }
}

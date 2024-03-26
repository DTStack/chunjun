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

import com.dtstack.chunjun.connector.hbase.FunctionParser;
import com.dtstack.chunjun.connector.hbase.FunctionTree;
import com.dtstack.chunjun.connector.hbase.config.HBaseConfig;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.dtstack.chunjun.connector.hbase.HBaseTypeUtils.MAX_TIMESTAMP_PRECISION;
import static com.dtstack.chunjun.connector.hbase.HBaseTypeUtils.MAX_TIME_PRECISION;
import static com.dtstack.chunjun.connector.hbase.HBaseTypeUtils.MIN_TIMESTAMP_PRECISION;
import static com.dtstack.chunjun.connector.hbase.HBaseTypeUtils.MIN_TIME_PRECISION;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

/** FlatRowConverter for sync task when add transformer */
@Slf4j
public class HBaseFlatRowConverter
        extends AbstractRowConverter<Result, RowData, Mutation, LogicalType> {

    public static final String KEY_ROW_KEY = "rowkey";
    private static final long serialVersionUID = 5955947079744643881L;

    private FunctionTree functionTree;

    private List<Integer> rowKeyColumnIndex;

    private final String encoding;

    private final Integer versionColumnIndex;

    private final String versionColumnValue;

    private final SimpleDateFormat timeSecondFormat =
            getSimpleDateFormat(ConstantValue.TIME_SECOND_SUFFIX);
    private final SimpleDateFormat timeMillisecondFormat =
            getSimpleDateFormat(ConstantValue.TIME_MILLISECOND_SUFFIX);

    private int rowKeyIndex = -1;

    private final List<String> columnNames = new ArrayList<>();

    private final HBaseConfig hBaseConfig;

    private List<String> rowKeyColumns;

    private final String nullMode;

    private final byte[][][] familyAndQualifier;

    public HBaseFlatRowConverter(HBaseConfig hBaseConfig, RowType rowType) {
        super(rowType);
        this.commonConfig = hBaseConfig;

        nullMode = hBaseConfig.getNullMode();
        encoding =
                StringUtils.isEmpty(hBaseConfig.getEncoding())
                        ? "utf-8"
                        : hBaseConfig.getEncoding();

        for (int i = 0; i < hBaseConfig.getColumn().size(); i++) {
            toExternalConverters.add(
                    i,
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(rowType.getTypeAt(i)), rowType.getTypeAt(i)));
        }
        this.familyAndQualifier = new byte[rowType.getFieldCount()][][];
        for (int i = 0; i < hBaseConfig.getColumn().size(); i++) {
            String name = hBaseConfig.getColumn().get(i).getName();
            columnNames.add(name);
            String[] cfAndQualifier = name.split(":");
            if (cfAndQualifier.length == 2
                    && StringUtils.isNotBlank(cfAndQualifier[0])
                    && StringUtils.isNotBlank(cfAndQualifier[1])) {

                byte[][] qualifierKeys = new byte[2][];
                qualifierKeys[0] = Bytes.toBytes(cfAndQualifier[0]);
                qualifierKeys[1] = Bytes.toBytes(cfAndQualifier[1]);
                familyAndQualifier[i] = qualifierKeys;
            } else if (KEY_ROW_KEY.equals(name)) {
                rowKeyIndex = i;
            } else {
                throw new IllegalArgumentException(
                        "hbase 中，column 的列配置格式应该是：列族:列名. 您配置的列错误：" + name);
            }
        }

        this.hBaseConfig = hBaseConfig;
        initRowKeyConfig();
        this.versionColumnIndex = hBaseConfig.getVersionColumnIndex();
        this.versionColumnValue = hBaseConfig.getVersionColumnValue();
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

        for (int i = 0; i < fieldTypes.length; i++) {
            if (rowKeyIndex == i) {
                continue;
            }
            toExternalConverters.get(i).serialize(rowData, i, put);
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
    public RowData toInternal(Result input) throws Exception {
        throw new ChunJunRuntimeException("This Hbase Convertor doesn't support toInternal.");
    }

    @Override
    public RowData toInternalLookup(RowData input) {
        throw new ChunJunRuntimeException("Hbase Connector doesn't support Lookup Table Function.");
    }

    @Override
    protected ISerializationConverter<Mutation> createExternalConverter(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return (rowData, pos, output) -> {
                    byte[] bytes = Bytes.toBytes(rowData.getBoolean(pos));
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            case INTERVAL_DAY_TIME:
            case BIGINT:
                return (rowData, pos, output) -> {
                    byte[] bytes = Bytes.toBytes(rowData.getLong(pos));
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            case SMALLINT:
                return (rowData, pos, output) -> {
                    byte[] bytes = Bytes.toBytes(rowData.getShort(pos));
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            case DOUBLE:
                return (rowData, pos, output) -> {
                    byte[] bytes = Bytes.toBytes(rowData.getDouble(pos));
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            case FLOAT:
                return (rowData, pos, output) -> {
                    byte[] bytes = Bytes.toBytes(rowData.getFloat(pos));
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            case DECIMAL:
                return (rowData, pos, output) -> {
                    DecimalType decimalType = (DecimalType) logicalType;
                    final int precision = decimalType.getPrecision();
                    final int scale = decimalType.getScale();
                    byte[] bytes =
                            Bytes.toBytes(rowData.getDecimal(pos, precision, scale).toBigDecimal());
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            case DATE:
            case INTERVAL_YEAR_MONTH:
            case INTEGER:
                return (rowData, pos, output) -> {
                    byte[] bytes = Bytes.toBytes(rowData.getInt(pos));
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            case CHAR:
            case VARCHAR:
                return (rowData, pos, output) -> {
                    byte[] bytes =
                            rowData.getString(pos).toString().getBytes(Charset.forName(encoding));
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            case BINARY:
            case VARBINARY:
                return (rowData, pos, output) -> {
                    byte[] bytes = rowData.getBinary(pos);
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
                    long millisecond =
                            rowData.getTimestamp(pos, timestampPrecision).getMillisecond();
                    byte[] bytes = Bytes.toBytes(millisecond);
                    ;
                    byte[][] qualifier = familyAndQualifier[pos];
                    ((Put) output).addColumn(qualifier[0], qualifier[1], bytes);
                };
            case TIME_WITHOUT_TIME_ZONE:
                return (rowData, pos, output) -> {
                    final int timePrecision = getPrecision(logicalType);
                    if (timePrecision < MIN_TIME_PRECISION || timePrecision > MAX_TIME_PRECISION) {
                        throw new UnsupportedOperationException(
                                String.format(
                                        "The precision %s of TIME type is out of the range [%s, %s] supported by "
                                                + "HBase connector",
                                        timePrecision, MIN_TIME_PRECISION, MAX_TIME_PRECISION));
                    }
                    byte[] bytes = Bytes.toBytes(rowData.getInt(pos));
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
                    ((GenericRowData) record).getField(keyColumnIndex));
        }

        String rowKeyStr = functionTree.evaluate(nameValueMap);
        return rowKeyStr.getBytes(StandardCharsets.UTF_8);
    }

    private void initRowKeyConfig() {
        if (StringUtils.isNotBlank(hBaseConfig.getRowkeyExpress())) {
            this.functionTree = FunctionParser.parse(hBaseConfig.getRowkeyExpress());
            this.rowKeyColumns = FunctionParser.parseRowKeyCol(hBaseConfig.getRowkeyExpress());
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
}

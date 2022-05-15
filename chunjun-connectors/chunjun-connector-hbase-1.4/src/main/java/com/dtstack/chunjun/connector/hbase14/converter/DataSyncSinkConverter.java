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

import com.dtstack.chunjun.connector.hbase14.sink.FunctionParser;
import com.dtstack.chunjun.connector.hbase14.sink.FunctionTree;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.enums.ColumnType;
import com.dtstack.chunjun.throwable.WriteRecordException;
import com.dtstack.chunjun.util.DateUtil;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @program chunjun
 * @author: wuren
 * @create: 2021/10/19
 */
public class DataSyncSinkConverter implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DataSyncSinkConverter.class);

    private boolean walFlag;
    private String nullMode;
    private String encoding;

    private List<String> columnTypes;
    private List<String> columnNames;
    private Integer versionColumnIndex;

    private String versionColumnValue;
    private List<String> rowKeyColumns;
    private List<Integer> rowKeyColumnIndex;

    private transient FunctionTree functionTree;
    private transient Map<String, String[]> nameMaps;
    private transient Map<String, byte[][]> nameByteMaps;
    private transient ThreadLocal<SimpleDateFormat> timeSecondFormatThreadLocal;
    private transient ThreadLocal<SimpleDateFormat> timeMillisecondFormatThreadLocal;

    public DataSyncSinkConverter(
            boolean walFlag,
            String nullMode,
            String encoding,
            List<String> columnTypes,
            List<String> columnNames,
            String rowkeyExpress,
            Integer versionColumnIndex,
            String versionColumnValue) {
        this.walFlag = walFlag;
        this.nullMode = nullMode;
        this.encoding = encoding;
        this.columnTypes = columnTypes;
        this.columnNames = columnNames;
        this.versionColumnIndex = versionColumnIndex;
        this.versionColumnValue = versionColumnValue;

        this.rowKeyColumns = Lists.newArrayList();
        this.rowKeyColumnIndex = Lists.newArrayList();

        this.nameMaps = Maps.newConcurrentMap();
        this.nameByteMaps = Maps.newConcurrentMap();
        timeSecondFormatThreadLocal = new ThreadLocal();
        timeMillisecondFormatThreadLocal = new ThreadLocal();

        this.functionTree = FunctionParser.parse(rowkeyExpress);
        this.rowKeyColumns = FunctionParser.parseRowKeyCol(rowkeyExpress);
        for (String rowKeyColumn : rowKeyColumns) {
            int index = columnNames.indexOf(rowKeyColumn);
            if (index == -1) {
                throw new RuntimeException(
                        "Can not get row key column from columns:" + rowKeyColumn);
            }
            rowKeyColumnIndex.add(index);
        }
    }

    public Put generatePutCommand(RowData rowData) throws WriteRecordException {
        int i = 0;
        try {
            byte[] rowkey = getRowkey(rowData);
            Put put;
            if (versionColumnIndex == null) {
                put = new Put(rowkey);
                if (!walFlag) {
                    put.setDurability(Durability.SKIP_WAL);
                }
            } else {
                long timestamp = getVersion(rowData);
                put = new Put(rowkey, timestamp);
            }

            for (; i < rowData.getArity(); ++i) {
                if (rowKeyColumnIndex.contains(i)) {
                    continue;
                }

                String type = columnTypes.get(i);
                String name = columnNames.get(i);
                String[] cfAndQualifier = nameMaps.get(name);
                byte[][] cfAndQualifierBytes = nameByteMaps.get(name);
                if (cfAndQualifier == null || cfAndQualifierBytes == null) {
                    cfAndQualifier = name.split(":");
                    if (cfAndQualifier.length == 2
                            && StringUtils.isNotBlank(cfAndQualifier[0])
                            && StringUtils.isNotBlank(cfAndQualifier[1])) {
                        nameMaps.put(name, cfAndQualifier);
                        cfAndQualifierBytes = new byte[2][];
                        cfAndQualifierBytes[0] = Bytes.toBytes(cfAndQualifier[0]);
                        cfAndQualifierBytes[1] = Bytes.toBytes(cfAndQualifier[1]);
                        nameByteMaps.put(name, cfAndQualifierBytes);
                    } else {
                        throw new IllegalArgumentException(
                                "Hbasewriter 中，column 的列配置格式应该是：列族:列名. 您配置的列错误：" + name);
                    }
                }

                ColumnType columnType = ColumnType.getType(type);
                Object column = null;
                if (rowData instanceof GenericRowData) {
                    column = ((GenericRowData) rowData).getField(i);
                } else if (rowData instanceof ColumnRowData) {
                    column = ((ColumnRowData) rowData).getField(i);
                }
                byte[] columnBytes = getColumnByte(columnType, column);
                // columnBytes 为null忽略这列
                if (null != columnBytes) {
                    put.addColumn(cfAndQualifierBytes[0], cfAndQualifierBytes[1], columnBytes);
                }
            }
            return put;
        } catch (Exception ex) {
            if (i < rowData.getArity()) {
                throw new WriteRecordException(ex.getMessage(), ex, i, rowData);
            }
            throw new WriteRecordException(ex.getMessage(), ex);
        }
    }

    private byte[] getRowkey(RowData record) throws Exception {
        Map<String, Object> nameValueMap = new HashMap<>((rowKeyColumnIndex.size() << 2) / 3);
        for (Integer keyColumnIndex : rowKeyColumnIndex) {
            Object column = null;
            if (record instanceof GenericRowData) {
                column = ((GenericRowData) record).getField(keyColumnIndex);
            } else if (record instanceof ColumnRowData) {
                column = ((ColumnRowData) record).getField(keyColumnIndex);
            }
            nameValueMap.put(columnNames.get(keyColumnIndex), column);
        }

        String rowKeyStr = functionTree.evaluate(nameValueMap);
        return rowKeyStr.getBytes(StandardCharsets.UTF_8);
    }

    public long getVersion(RowData rawRecord) {
        RowData record = rawRecord;
        Integer index = versionColumnIndex.intValue();
        long timestamp;
        if (index == null) {
            // 指定时间作为版本
            timestamp = Long.valueOf(versionColumnValue);
            if (timestamp < 0) {
                throw new IllegalArgumentException(
                        "Illegal timestamp to construct versionClumn: " + timestamp);
            }
        } else {
            // 指定列作为版本,long/doubleColumn直接record.aslong, 其它类型尝试用yyyy-MM-dd HH:mm:ss,yyyy-MM-dd
            // HH:mm:ss SSS去format
            if (index >= record.getArity() || index < 0) {
                throw new IllegalArgumentException("version column index out of range: " + index);
            }
            Object column = null;
            if (record instanceof GenericRowData) {
                column = ((GenericRowData) record).getField(index);
            } else if (record instanceof ColumnRowData) {
                column = ((ColumnRowData) record).getField(index);
            }
            if (column == null) {
                throw new IllegalArgumentException("null verison column!");
            }
            SimpleDateFormat dfSeconds = getSimpleDateFormat(ConstantValue.TIME_SECOND_SUFFIX);
            SimpleDateFormat dfMs = getSimpleDateFormat(ConstantValue.TIME_MILLISECOND_SUFFIX);

            if (column instanceof Long) {
                Long longValue = (Long) column;
                timestamp = longValue;
            } else if (column instanceof Double) {
                Double doubleValue = (Double) column;
                timestamp = doubleValue.longValue();
            } else if (column instanceof String) {
                Date date;
                try {

                    date = dfMs.parse((String) column);
                } catch (ParseException e) {
                    try {
                        date = dfSeconds.parse((String) column);
                    } catch (ParseException e1) {
                        LOG.info(
                                String.format(
                                        "您指定第[%s]列作为hbase写入版本,但在尝试用yyyy-MM-dd HH:mm:ss 和 yyyy-MM-dd HH:mm:ss SSS 去解析为Date时均出错,请检查并修改",
                                        index));
                        throw new RuntimeException(e1);
                    }
                }
                timestamp = date.getTime();
            } else if (column instanceof Date) {
                timestamp = ((Date) column).getTime();
            } else if (column instanceof BigDecimalColumn) {
                timestamp = ((BigDecimalColumn) column).asLong();
            } else {
                throw new RuntimeException("rowkey类型不兼容: " + column.getClass());
            }
        }
        return timestamp;
    }

    public byte[] getColumnByte(ColumnType columnType, Object column) {
        byte[] bytes;
        if (column != null) {
            switch (columnType) {
                case INT:
                    bytes = DataSyncConverterUtils.intToBytes(column);
                    break;
                case LONG:
                    bytes = DataSyncConverterUtils.longToBytes(column);
                    break;
                case DOUBLE:
                    bytes = DataSyncConverterUtils.doubleToBytes(column);
                    break;
                case FLOAT:
                    bytes = DataSyncConverterUtils.floatToBytes(column);
                    break;
                case SHORT:
                    bytes = DataSyncConverterUtils.shortToBytes(column);
                    break;
                case BOOLEAN:
                    bytes = DataSyncConverterUtils.boolToBytes(column);
                    break;
                case STRING:
                    String stringValue;
                    if (column instanceof Timestamp) {
                        SimpleDateFormat fm = DateUtil.getDateTimeFormatter();
                        stringValue = fm.format(column);
                    } else {
                        stringValue = String.valueOf(column);
                    }
                    bytes = DataSyncConverterUtils.getValueByte(columnType, stringValue, encoding);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported column type: " + columnType);
            }
        } else {
            switch (nullMode.toUpperCase()) {
                case "SKIP":
                    bytes = null;
                    break;
                case "EMPTY":
                    bytes = HConstants.EMPTY_BYTE_ARRAY;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported null mode: " + nullMode);
            }
        }
        return bytes;
    }

    private SimpleDateFormat getSimpleDateFormat(String sign) {
        SimpleDateFormat format;
        if (ConstantValue.TIME_SECOND_SUFFIX.equals(sign)) {
            format = timeSecondFormatThreadLocal.get();
            if (format == null) {
                format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                timeSecondFormatThreadLocal.set(format);
            }
        } else {
            format = timeMillisecondFormatThreadLocal.get();
            if (format == null) {
                format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
                timeMillisecondFormatThreadLocal.set(format);
            }
        }

        return format;
    }

    public void close() {
        if (null != timeSecondFormatThreadLocal) {
            timeSecondFormatThreadLocal.remove();
        }

        if (null != timeMillisecondFormatThreadLocal) {
            timeMillisecondFormatThreadLocal.remove();
        }
    }
}

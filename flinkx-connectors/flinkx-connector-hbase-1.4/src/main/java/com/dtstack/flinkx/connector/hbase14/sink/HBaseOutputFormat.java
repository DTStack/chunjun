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

package com.dtstack.flinkx.connector.hbase14.sink;

import com.dtstack.flinkx.connector.hbase14.util.HBaseHelper;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.sink.format.BaseRichOutputFormat;
import com.dtstack.flinkx.throwable.WriteRecordException;
import com.dtstack.flinkx.util.DateUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The Hbase Implementation of OutputFormat
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class HBaseOutputFormat extends BaseRichOutputFormat {

    protected Map<String, Object> hbaseConfig;

    protected String tableName;

    protected String encoding;

    protected String nullMode;

    protected boolean walFlag;

    protected long writeBufferSize;

    protected List<String> columnTypes;

    protected List<String> columnNames;

    protected String rowkeyExpress;

    protected Integer versionColumnIndex;

    protected String versionColumnValue;
    protected List<String> rowKeyColumns = Lists.newArrayList();
    protected List<Integer> rowKeyColumnIndex = Lists.newArrayList();
    private transient Connection connection;
    private transient BufferedMutator bufferedMutator;
    private transient FunctionTree functionTree;
    private transient Map<String, String[]> nameMaps;

    private transient Map<String, byte[][]> nameByteMaps;

    private transient ThreadLocal<SimpleDateFormat> timeSecondFormatThreadLocal;

    private transient ThreadLocal<SimpleDateFormat> timeMillisecondFormatThreadLocal;

    private boolean openKerberos = false;
    private transient Table table;

    @Override
    public void configure(Configuration parameters) {}

    private Put generatePutCommand(RowData rowData) throws WriteRecordException {
        RowData record = rowData;
        int i = 0;
        try {
            byte[] rowkey = getRowkey(record);
            Put put;
            if (versionColumnIndex == null) {
                put = new Put(rowkey);
                if (!walFlag) {
                    put.setDurability(Durability.SKIP_WAL);
                }
            } else {
                long timestamp = getVersion(record);
                put = new Put(rowkey, timestamp);
            }

            for (; i < record.getArity(); ++i) {
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
                if (record instanceof GenericRowData) {
                    column = ((GenericRowData) record).getField(i);
                } else if (record instanceof ColumnRowData) {
                    column = ((ColumnRowData) record).getField(i);
                }
                byte[] columnBytes = getColumnByte(columnType, column);
                // columnBytes 为null忽略这列
                if (null != columnBytes) {
                    put.addColumn(cfAndQualifierBytes[0], cfAndQualifierBytes[1], columnBytes);
                }
            }
            return put;
        } catch (Exception ex) {
            if (i < record.getArity()) {
                throw new WriteRecordException(
                        recordConvertDetailErrorMessage(i, record), ex, i, record);
            }
            throw new WriteRecordException(ex.getMessage(), ex);
        }
    }

    @Override
    protected void writeSingleRecordInternal(RowData rawRecord) throws WriteRecordException {
        int i = 0;
        try {
            bufferedMutator.mutate(generatePutCommand(rawRecord));
        } catch (Exception ex) {
            if (i < rawRecord.getArity()) {
                throw new WriteRecordException(
                        recordConvertDetailErrorMessage(i, rawRecord), ex, i, rawRecord);
            }
            throw new WriteRecordException(ex.getMessage(), ex);
        }
    }

    @Override
    public void openInternal(int taskNumber, int numTasks) throws IOException {
        openKerberos = HBaseHelper.openKerberos(hbaseConfig);
        if (openKerberos) {
            sleepRandomTime();

            UserGroupInformation ugi = HBaseHelper.getUgi(hbaseConfig);
            ugi.doAs(
                    (PrivilegedAction<Object>)
                            () -> {
                                openConnection();
                                return null;
                            });
        } else {
            openConnection();
        }
    }

    private void sleepRandomTime() {
        try {
            Thread.sleep(5000L + (long) (10000 * Math.random()));
        } catch (Exception exception) {
            LOG.warn("", exception);
        }
    }

    public void openConnection() {
        LOG.info("HbaseOutputFormat configure start");
        nameMaps = Maps.newConcurrentMap();
        nameByteMaps = Maps.newConcurrentMap();
        timeSecondFormatThreadLocal = new ThreadLocal();
        timeMillisecondFormatThreadLocal = new ThreadLocal();
        Validate.isTrue(hbaseConfig != null && hbaseConfig.size() != 0, "hbaseConfig不能为空Map结构!");

        try {
            org.apache.hadoop.conf.Configuration hConfiguration =
                    HBaseHelper.getConfig(hbaseConfig);
            connection = ConnectionFactory.createConnection(hConfiguration);

            bufferedMutator =
                    connection.getBufferedMutator(
                            new BufferedMutatorParams(TableName.valueOf(tableName))
                                    .pool(HTable.getDefaultExecutor(hConfiguration))
                                    .writeBufferSize(writeBufferSize));
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (Exception e) {
            HBaseHelper.closeBufferedMutator(bufferedMutator);
            HBaseHelper.closeConnection(connection);
            throw new IllegalArgumentException(e);
        }
        functionTree = FunctionParser.parse(rowkeyExpress);
        rowKeyColumns = FunctionParser.parseRowKeyCol(rowkeyExpress);
        for (String rowKeyColumn : rowKeyColumns) {
            int index = columnNames.indexOf(rowKeyColumn);
            if (index == -1) {
                throw new RuntimeException(
                        "Can not get row key column from columns:" + rowKeyColumn);
            }
            rowKeyColumnIndex.add(index);
        }

        LOG.info("HbaseOutputFormat configure end");
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

    protected String recordConvertDetailErrorMessage(int pos, RowData row) {
        return "\nHbaseOutputFormat ["
                + jobName
                + "] writeRecord error: when converting field["
                + columnNames.get(pos)
                + "] in Row("
                + row
                + ")";
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        Object[] results = new Object[0];
        try {
            List<Put> puts = new ArrayList<>();
            for (RowData record : rows) {
                puts.add(generatePutCommand(record));
            }
            results = new Object[puts.size()];
            table.batch(puts, results);
        } catch (IOException e) {
            throw new IOException(e);
        } finally {
            for (int i = 0; i < Objects.requireNonNull(results).length; i++) {
                if (results[i] instanceof Exception) {
                    Exception exception = (Exception) results[i];
                    LOG.error(exception.getMessage(), exception);
                }
            }
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

    public byte[] getValueByte(ColumnType columnType, String value) {
        byte[] bytes;
        if (value != null) {
            switch (columnType) {
                case INT:
                    bytes = Bytes.toBytes(Integer.parseInt(value));
                    break;
                case LONG:
                    bytes = Bytes.toBytes(Long.parseLong(value));
                    break;
                case DOUBLE:
                    bytes = Bytes.toBytes(Double.parseDouble(value));
                    break;
                case FLOAT:
                    bytes = Bytes.toBytes(Float.parseFloat(value));
                    break;
                case SHORT:
                    bytes = Bytes.toBytes(Short.parseShort(value));
                    break;
                case BOOLEAN:
                    bytes = Bytes.toBytes(Boolean.parseBoolean(value));
                    break;
                case STRING:
                    bytes = value.getBytes(Charset.forName(encoding));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported column type: " + columnType);
            }
        } else {
            bytes = HConstants.EMPTY_BYTE_ARRAY;
        }
        return bytes;
    }

    public byte[] getColumnByte(ColumnType columnType, Object column) {
        byte[] bytes;
        if (column != null) {
            switch (columnType) {
                case INT:
                    bytes = intToBytes(column);
                    break;
                case LONG:
                    bytes = longToBytes(column);
                    break;
                case DOUBLE:
                    bytes = doubleToBytes(column);
                    break;
                case FLOAT:
                    bytes = floatToBytes(column);
                    break;
                case SHORT:
                    bytes = shortToBytes(column);
                    break;
                case BOOLEAN:
                    bytes = boolToBytes(column);
                    break;
                case STRING:
                    String stringValue;
                    if (column instanceof Timestamp) {
                        SimpleDateFormat fm = DateUtil.getDateTimeFormatter();
                        stringValue = fm.format(column);
                    } else {
                        stringValue = String.valueOf(column);
                    }
                    bytes = this.getValueByte(columnType, stringValue);
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

    private byte[] intToBytes(Object column) {
        Integer intValue = null;
        if (column instanceof Integer) {
            intValue = (Integer) column;
        } else if (column instanceof Long) {
            intValue = ((Long) column).intValue();
        } else if (column instanceof Double) {
            intValue = ((Double) column).intValue();
        } else if (column instanceof Float) {
            intValue = ((Float) column).intValue();
        } else if (column instanceof Short) {
            intValue = ((Short) column).intValue();
        } else if (column instanceof Boolean) {
            intValue = (Boolean) column ? 1 : 0;
        } else if (column instanceof String) {
            intValue = Integer.valueOf((String) column);
        } else {
            throw new RuntimeException("Can't convert from " + column.getClass() + " to INT");
        }

        return Bytes.toBytes(intValue);
    }

    private byte[] longToBytes(Object column) {
        Long longValue = null;
        if (column instanceof Integer) {
            longValue = ((Integer) column).longValue();
        } else if (column instanceof Long) {
            longValue = (Long) column;
        } else if (column instanceof Double) {
            longValue = ((Double) column).longValue();
        } else if (column instanceof Float) {
            longValue = ((Float) column).longValue();
        } else if (column instanceof Short) {
            longValue = ((Short) column).longValue();
        } else if (column instanceof Boolean) {
            longValue = (Boolean) column ? 1L : 0L;
        } else if (column instanceof String) {
            longValue = Long.valueOf((String) column);
        } else if (column instanceof Timestamp) {
            longValue = ((Timestamp) column).getTime();
        } else {
            throw new RuntimeException("Can't convert from " + column.getClass() + " to LONG");
        }

        return Bytes.toBytes(longValue);
    }

    private byte[] doubleToBytes(Object column) {
        Double doubleValue = null;
        if (column instanceof Integer) {
            doubleValue = ((Integer) column).doubleValue();
        } else if (column instanceof Long) {
            doubleValue = ((Long) column).doubleValue();
        } else if (column instanceof Double) {
            doubleValue = (Double) column;
        } else if (column instanceof Float) {
            doubleValue = ((Float) column).doubleValue();
        } else if (column instanceof Short) {
            doubleValue = ((Short) column).doubleValue();
        } else if (column instanceof Boolean) {
            doubleValue = (Boolean) column ? 1.0 : 0.0;
        } else if (column instanceof String) {
            doubleValue = Double.valueOf((String) column);
        } else {
            throw new RuntimeException("Can't convert from " + column.getClass() + " to DOUBLE");
        }

        return Bytes.toBytes(doubleValue);
    }

    private byte[] floatToBytes(Object column) {
        Float floatValue = null;
        if (column instanceof Integer) {
            floatValue = ((Integer) column).floatValue();
        } else if (column instanceof Long) {
            floatValue = ((Long) column).floatValue();
        } else if (column instanceof Double) {
            floatValue = ((Double) column).floatValue();
        } else if (column instanceof Float) {
            floatValue = (Float) column;
        } else if (column instanceof Short) {
            floatValue = ((Short) column).floatValue();
        } else if (column instanceof Boolean) {
            floatValue = (Boolean) column ? 1.0f : 0.0f;
        } else if (column instanceof String) {
            floatValue = Float.valueOf((String) column);
        } else {
            throw new RuntimeException("Can't convert from " + column.getClass() + " to DOUBLE");
        }

        return Bytes.toBytes(floatValue);
    }

    private byte[] shortToBytes(Object column) {
        Short shortValue = null;
        if (column instanceof Integer) {
            shortValue = ((Integer) column).shortValue();
        } else if (column instanceof Long) {
            shortValue = ((Long) column).shortValue();
        } else if (column instanceof Double) {
            shortValue = ((Double) column).shortValue();
        } else if (column instanceof Float) {
            shortValue = ((Float) column).shortValue();
        } else if (column instanceof Short) {
            shortValue = (Short) column;
        } else if (column instanceof Boolean) {
            shortValue = (Boolean) column ? (short) 1 : (short) 0;
        } else if (column instanceof String) {
            shortValue = Short.valueOf((String) column);
        } else {
            throw new RuntimeException("Can't convert from " + column.getClass() + " to SHORT");
        }
        return Bytes.toBytes(shortValue);
    }

    private byte[] boolToBytes(Object column) {
        Boolean booleanValue = null;
        if (column instanceof Integer) {
            booleanValue = (Integer) column != 0;
        } else if (column instanceof Long) {
            booleanValue = (Long) column != 0L;
        } else if (column instanceof Double) {
            booleanValue = new Double(0.0).compareTo((Double) column) != 0;
        } else if (column instanceof Float) {
            booleanValue = new Float(0.0f).compareTo((Float) column) != 0;
        } else if (column instanceof Short) {
            booleanValue = (Short) column != 0;
        } else if (column instanceof Boolean) {
            booleanValue = (Boolean) column;
        } else if (column instanceof String) {
            booleanValue = Boolean.valueOf((String) column);
        } else {
            throw new RuntimeException("Can't convert from " + column.getClass() + " to SHORT");
        }

        return Bytes.toBytes(booleanValue);
    }

    @Override
    public void closeInternal() throws IOException {
        if (null != timeSecondFormatThreadLocal) {
            timeSecondFormatThreadLocal.remove();
        }

        if (null != timeMillisecondFormatThreadLocal) {
            timeMillisecondFormatThreadLocal.remove();
        }

        HBaseHelper.closeBufferedMutator(bufferedMutator);
        HBaseHelper.closeConnection(connection);
    }
}

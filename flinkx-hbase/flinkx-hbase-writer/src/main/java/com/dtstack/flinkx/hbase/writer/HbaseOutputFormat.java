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

package com.dtstack.flinkx.hbase.writer;

import com.dtstack.flinkx.authenticate.KerberosUtil;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.hbase.HbaseHelper;
import com.dtstack.flinkx.hbase.writer.function.FunctionParser;
import com.dtstack.flinkx.hbase.writer.function.FunctionTree;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import com.dtstack.flinkx.util.DateUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.Validate;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Maps;

/**
 * The Hbase Implementation of OutputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class HbaseOutputFormat extends RichOutputFormat {

    private String jobName = "defaultJobName";

    protected Map<String,Object> hbaseConfig;

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

    private transient Connection connection;

    private transient BufferedMutator bufferedMutator;

    private transient FunctionTree functionTree;

    protected List<String> rowKeyColumns = Lists.newArrayList();
    protected List<Integer> rowKeyColumnIndex = Lists.newArrayList();

    private transient Map<String,String[]> nameMaps;

    private transient Map<String, byte[][]> nameByteMaps ;

    private transient ThreadLocal<SimpleDateFormat> timesssFormatThreadLocal;

    private transient ThreadLocal<SimpleDateFormat> timeSSSFormatThreadLocal;

    private boolean openKerberos = false;

    @Override
    public void configure(Configuration parameters) {
        LOG.info("HbaseOutputFormat configure start");
        nameMaps = Maps.newConcurrentMap();
        nameByteMaps = Maps.newConcurrentMap();
        timesssFormatThreadLocal = new ThreadLocal();
        timeSSSFormatThreadLocal = new ThreadLocal();
        Validate.isTrue(hbaseConfig != null && hbaseConfig.size() !=0, "hbaseConfig不能为空Map结构!");

        try {
            connection = HbaseHelper.getHbaseConnection(hbaseConfig, jobId, "writer");

            org.apache.hadoop.conf.Configuration hConfiguration = HbaseHelper.getConfig(hbaseConfig);
            bufferedMutator = connection.getBufferedMutator(
                    new BufferedMutatorParams(TableName.valueOf(tableName))
                            .pool(HTable.getDefaultExecutor(hConfiguration))
                            .writeBufferSize(writeBufferSize));
        } catch (Exception e) {
            HbaseHelper.closeBufferedMutator(bufferedMutator);
            HbaseHelper.closeConnection(connection);
            throw new IllegalArgumentException(e);
        }

        functionTree = FunctionParser.parse(rowkeyExpress);
        rowKeyColumns = FunctionParser.parseRowKeyCol(rowkeyExpress);
        for (String rowKeyColumn : rowKeyColumns) {
            int index = columnNames.indexOf(rowKeyColumn);
            if(index == -1){
                throw new RuntimeException("Can not get row key column from columns:" + rowKeyColumn);
            }
            rowKeyColumnIndex.add(index);
        }

        LOG.info("HbaseOutputFormat configure end");
    }

    @Override
    public void openInternal(int taskNumber, int numTasks) throws IOException {
        openKerberos = HbaseHelper.openKerberos(hbaseConfig);
    }

    @Override
    public void writeSingleRecordInternal(Row record) throws WriteRecordException {
        int i = 0;
        try {
            byte[] rowkey = getRowkey(record);
            Put put;
            if(versionColumnIndex == null) {
                put = new Put(rowkey);
                if(!walFlag) {
                    put.setDurability(Durability.SKIP_WAL);
                }
            } else {
                long timestamp = getVersion(record);
                put = new Put(rowkey,timestamp);
            }

            for (; i < record.getArity(); ++i) {
                if(rowKeyColumnIndex.contains(i)){
                    continue;
                }

                String type = columnTypes.get(i);
                ColumnType columnType = ColumnType.getByTypeName(type);
                String name =columnNames.get(i);
                String[] cfAndQualifier = nameMaps.get(name);
                byte[][] cfAndQualifierBytes = nameByteMaps.get(name);
                if(cfAndQualifier == null || cfAndQualifierBytes==null){
                    String promptInfo = "Hbasewriter 中，column 的列配置格式应该是：列族:列名. 您配置的列错误：" + name;
                    cfAndQualifier = name.split(":");
                    Validate.isTrue(cfAndQualifier != null && cfAndQualifier.length == 2
                            && org.apache.commons.lang3.StringUtils.isNotBlank(cfAndQualifier[0])
                            && org.apache.commons.lang3.StringUtils.isNotBlank(cfAndQualifier[1]), promptInfo);
                    nameMaps.put(name,cfAndQualifier);
                    cfAndQualifierBytes = new byte[2][];
                    cfAndQualifierBytes[0] = Bytes.toBytes(cfAndQualifier[0]);
                    cfAndQualifierBytes[1] = Bytes.toBytes(cfAndQualifier[1]);
                    nameByteMaps.put(name,cfAndQualifierBytes);
                }
                byte[] columnBytes = getColumnByte(columnType,record.getField(i));
                //columnBytes 为null忽略这列
                if(null != columnBytes){
                    put.addColumn(
                            cfAndQualifierBytes[0],
                            cfAndQualifierBytes[1],
                            columnBytes);
                }else{
                    continue;
                }
            }

            bufferedMutator.mutate(put);
        } catch(Exception ex) {
            if(i < record.getArity()) {
                throw new WriteRecordException(recordConvertDetailErrorMessage(i, record), ex, i, record);
            }
            throw new WriteRecordException(ex.getMessage(), ex);
        }
    }

    private SimpleDateFormat getSimpleDateFormat(String sign){
        SimpleDateFormat format = null;
        if("sss".equalsIgnoreCase(sign)){
            format = timesssFormatThreadLocal.get();
            if(format == null){
                format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                timesssFormatThreadLocal.set(format);
            }
        }else if("SSS".equalsIgnoreCase(sign)){
            format = timeSSSFormatThreadLocal.get();
            if(format == null){
                format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
                timeSSSFormatThreadLocal.set(format);
            }
        }
        return format;
    }

    @Override
    protected String recordConvertDetailErrorMessage(int pos, Row row) {
        return "\nHbaseOutputFormat [" + jobName + "] writeRecord error: when converting field[" + columnNames.get(pos) + "] in Row(" + row + ")";
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        throw new IllegalArgumentException();
    }

    private byte[] getRowkey(Row record) {
        Map<String, Object> nameValueMap = new HashMap<>();
        for (Integer keyColumnIndex : rowKeyColumnIndex) {
            nameValueMap.put(columnNames.get(keyColumnIndex), record.getField(keyColumnIndex));
        }

        String rowKeyStr = functionTree.evaluate(nameValueMap);
        return rowKeyStr.getBytes();
    }

    public long getVersion(Row record){
        Integer index = versionColumnIndex.intValue();
        long timestamp;
        if(index == null){
            //指定时间作为版本
            timestamp = Long.valueOf(versionColumnValue);
            if(timestamp < 0){
                throw new IllegalArgumentException("Illegal timestamp to construct versionClumn: " + timestamp);
            }
        }else{
            //指定列作为版本,long/doubleColumn直接record.aslong, 其它类型尝试用yyyy-MM-dd HH:mm:ss,yyyy-MM-dd HH:mm:ss SSS去format
            if(index >= record.getArity() || index < 0){
                throw new IllegalArgumentException("version column index out of range: " + index);
            }
            if(record.getField(index)  == null){
                throw new IllegalArgumentException("null verison column!");
            }
            SimpleDateFormat df_senconds = getSimpleDateFormat("sss");
            SimpleDateFormat df_ms = getSimpleDateFormat("SSS");
            Object column = record.getField(index);
            if(column instanceof Long){
                Long longValue = (Long) column;
                timestamp = longValue;
            } else if (column instanceof Double){
                Double doubleValue = (Double) column;
                timestamp = doubleValue.longValue();
            } else if (column instanceof String){
                Date date;
                try{

                    date = df_ms.parse((String) column);
                }catch (ParseException e){
                    try {
                        date = df_senconds.parse((String) column);
                    } catch (ParseException e1) {
                        LOG.info(String.format("您指定第[%s]列作为hbase写入版本,但在尝试用yyyy-MM-dd HH:mm:ss 和 yyyy-MM-dd HH:mm:ss SSS 去解析为Date时均出错,请检查并修改",index));
                        throw new RuntimeException(e1);
                    }
                }
                timestamp = date.getTime();
            } else if (column instanceof Date) {
                timestamp = ((Date) column).getTime();
            } else {
                throw new RuntimeException("rowkey类型不兼容: " + column.getClass());
            }
        }
        return timestamp;
    }

    public byte[] getValueByte(ColumnType columnType, String value){
        byte[] bytes;
        if(value != null){
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
        }else{
            bytes = HConstants.EMPTY_BYTE_ARRAY;
        }
        return  bytes;
    }

    public byte[] getColumnByte(ColumnType columnType, Object column){
        byte[] bytes;
        if(column != null){
            switch (columnType) {
                case INT:
                    Integer intValue = null;
                    if(column instanceof Integer) {
                        intValue = (Integer) column;
                    } else if(column instanceof Long) {
                        intValue = Integer.valueOf(((Long)column).intValue());
                    } else if(column instanceof Double) {
                        intValue = ((Double) column).intValue();
                    } else if(column instanceof Float) {
                        intValue = ((Float) column).intValue();
                    } else if(column instanceof  Short) {
                        intValue = ((Short) column).intValue();
                    } else if(column instanceof  Boolean) {
                        intValue = ((Boolean) column).booleanValue() ? 1 : 0;
                    } else if(column instanceof String) {
                        intValue = Integer.valueOf((String) column);
                    } else {
                        throw new RuntimeException("Can't convert from " + column.getClass() +  " to INT");
                    }
                    bytes = Bytes.toBytes(intValue);
                    break;
                case LONG:
                    Long longValue = null;
                    if(column instanceof Integer) {
                        longValue = ((Integer)column).longValue();
                    } else if(column instanceof Long) {
                        longValue = (Long) column;
                    } else if(column instanceof Double) {
                        longValue = ((Double) column).longValue();
                    } else if(column instanceof Float) {
                        longValue = ((Float) column).longValue();
                    } else if(column instanceof  Short) {
                        longValue = ((Short) column).longValue();
                    } else if(column instanceof  Boolean) {
                        longValue = ((Boolean) column).booleanValue() ? 1L : 0L;
                    } else if(column instanceof String) {
                        longValue = Long.valueOf((String) column);
                    }else if (column instanceof Timestamp){
                        longValue = ((Timestamp) column).getTime();
                    }else {
                        throw new RuntimeException("Can't convert from " + column.getClass() +  " to LONG");
                    }
                    bytes = Bytes.toBytes(longValue);
                    break;
                case DOUBLE:
                    Double doubleValue = null;
                    if(column instanceof Integer) {
                        doubleValue = ((Integer)column).doubleValue();
                    } else if(column instanceof Long) {
                        doubleValue = ((Long) column).doubleValue();
                    } else if(column instanceof Double) {
                        doubleValue = (Double) column;
                    } else if(column instanceof Float) {
                        doubleValue = ((Float) column).doubleValue();
                    } else if(column instanceof  Short) {
                        doubleValue = ((Short) column).doubleValue();
                    } else if(column instanceof  Boolean) {
                        doubleValue = ((Boolean) column).booleanValue() ? 1.0 : 0.0;
                    } else if(column instanceof String) {
                        doubleValue = Double.valueOf((String) column);
                    } else {
                        throw new RuntimeException("Can't convert from " + column.getClass() +  " to DOUBLE");
                    }
                    bytes = Bytes.toBytes(doubleValue);
                    break;
                case FLOAT:
                    Float floatValue = null;
                    if(column instanceof Integer) {
                        floatValue = ((Integer)column).floatValue();
                    } else if(column instanceof Long) {
                        floatValue = ((Long) column).floatValue();
                    } else if(column instanceof Double) {
                        floatValue = ((Double) column).floatValue();
                    } else if(column instanceof Float) {
                        floatValue = (Float) column;
                    } else if(column instanceof  Short) {
                        floatValue = ((Short) column).floatValue();
                    } else if(column instanceof  Boolean) {
                        floatValue = ((Boolean) column).booleanValue() ? 1.0f : 0.0f;
                    } else if(column instanceof String) {
                        floatValue = Float.valueOf((String) column);
                    } else {
                        throw new RuntimeException("Can't convert from " + column.getClass() +  " to DOUBLE");
                    }
                    bytes = Bytes.toBytes(floatValue);
                    break;
                case SHORT:
                    Short shortValue = null;
                    if(column instanceof Integer) {
                        shortValue = ((Integer)column).shortValue();
                    } else if(column instanceof Long) {
                        shortValue = ((Long) column).shortValue();
                    } else if(column instanceof Double) {
                        shortValue = ((Double) column).shortValue();
                    } else if(column instanceof Float) {
                        shortValue = ((Float) column).shortValue();
                    } else if(column instanceof  Short) {
                        shortValue = (Short) column;
                    } else if(column instanceof  Boolean) {
                        shortValue = ((Boolean) column).booleanValue() ? (short) 1 : (short) 0 ;
                    } else if(column instanceof String) {
                        shortValue = Short.valueOf((String) column);
                    } else {
                        throw new RuntimeException("Can't convert from " + column.getClass() +  " to SHORT");
                    }
                    bytes = Bytes.toBytes(shortValue);
                    break;
                case BOOLEAN:
                    Boolean booleanValue = null;
                    if(column instanceof Integer) {
                        booleanValue = (Integer)column == 0 ? false : true;
                    } else if(column instanceof Long) {
                        booleanValue = (Long) column == 0L ? false : true;
                    } else if(column instanceof Double) {
                        booleanValue = (Double) column == 0.0 ? false : true;
                    } else if(column instanceof Float) {
                        booleanValue = (Float) column == 0.0f ? false : true;
                    } else if(column instanceof  Short) {
                        booleanValue =  (Short) column == 0 ? false : true;
                    } else if(column instanceof  Boolean) {
                        booleanValue = (Boolean) column;
                    } else if(column instanceof String) {
                        booleanValue = Boolean.valueOf((String)column);
                    } else {
                        throw new RuntimeException("Can't convert from " + column.getClass() +  " to SHORT");
                    }
                    bytes = Bytes.toBytes(booleanValue);
                    break;
                case STRING:
                    String stringValue;
                    if (column instanceof Timestamp){
                        SimpleDateFormat fm = DateUtil.getDateTimeFormatter();
                        stringValue = fm.format(column);
                    }else {
                        stringValue = String.valueOf(column);
                    }
                    bytes = this.getValueByte(columnType, stringValue);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported column type: " + columnType);
            }
        }else{
            switch (nullMode.toUpperCase()){
                case "SKIP":
                    bytes =  null;
                    break;
                case "EMPTY":
                    bytes = HConstants.EMPTY_BYTE_ARRAY;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported null mode: " + nullMode);
            }
        }
        return  bytes;
    }

    @Override
    public void closeInternal() throws IOException {
        HbaseHelper.closeBufferedMutator(bufferedMutator);
        HbaseHelper.closeConnection(connection);

        if(openKerberos){
            KerberosUtil.clear(jobId);
        }
    }

}

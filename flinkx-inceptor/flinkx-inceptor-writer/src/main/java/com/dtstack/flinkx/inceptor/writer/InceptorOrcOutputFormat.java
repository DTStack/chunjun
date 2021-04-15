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


package com.dtstack.flinkx.inceptor.writer;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.inceptor.ECompressType;
import com.dtstack.flinkx.inceptor.HdfsUtil;
import com.dtstack.flinkx.util.ColumnTypeUtil;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.ReflectionUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.flink.types.Row;
import org.apache.hadoop.hive.common.Dialect;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarchar2Writable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hive.streaming.HiveStreamingConnection;
import org.apache.hive.streaming.StreamingException;
import org.apache.hive.streaming.StrictDelimitedInputWriter;
import org.apache.thrift.TException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.PrivilegedAction;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.inceptor.HdfsConfigKeys.COLUMN_FIELD_DELIMITER;

/**
 * The subclass of HdfsOutputFormat writing orc files
 * <p>
 * Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class InceptorOrcOutputFormat extends BaseInceptorOutputFormat {
    private RecordWriter recordWriter;
    private OrcSerde orcSerde;
    private StructObjectInspector inspector;
    private FileOutputFormat outputFormat;
    private JobConf jobConf;
    private StrictDelimitedInputWriter wr;
    private HiveConf hiveConf;
    private HiveMetaStoreClient hiveMetaStoreClient;
    private HiveStreamingConnection connection;
    private String schema;
    private String table;

    private static ColumnTypeUtil.DecimalInfo ORC_DEFAULT_DECIMAL_INFO = new ColumnTypeUtil.DecimalInfo(HiveDecimal.SYSTEM_DEFAULT_PRECISION, HiveDecimal.SYSTEM_DEFAULT_SCALE);

    private void initConnection() throws StreamingException {
        HiveStreamingConnection.Builder builder = HiveStreamingConnection.newBuilder()
                .withDatabase(schema)
                .withTable(table)
                .withAgentInfo("UT_" + Thread.currentThread().getName())
                .withTransactionBatchSize(1)
                .withRecordWriter(wr)
                .withHiveConf(hiveConf)
                .withClient(hiveMetaStoreClient);
        if (CollectionUtils.isNotEmpty(partitions)) {
            builder.withStaticPartitionValues(partitions);
        }
        try {
            hiveMetaStoreClient.dropPartition(schema, table, partitions);
        } catch (TException e) {
            LOG.error(e.getMessage(), e);
        }
        connection = builder.connect();
    }

    private void setFullColumns() throws Exception{
        fullColumnNames.clear();
        fullColumnTypes.clear();
        Table inceptorTable = hiveMetaStoreClient.getTable(schema,table);
        List<FieldSchema> cols = inceptorTable.getSd().getCols();
        if(CollectionUtils.isNotEmpty(cols)){
            cols.forEach(fieldSchema -> {
                fullColumnNames.add(fieldSchema.getName());
                fullColumnTypes.add(fieldSchema.getType());
            });
        }
    }

    @Override
    protected void openSource() throws IOException {
        super.openSource();
        if (isTransaction) {
            table = String.valueOf(hadoopConfig.get("table"));
            schema = String.valueOf(hadoopConfig.get("schema"));
            ugi.doAs(new PrivilegedAction<Void>() {
                public Void run() {
                    try {
                        hiveConf = new HiveConf();
                        hiveConf.addResource(conf);
                        wr = StrictDelimitedInputWriter.newBuilder()
                                .withFieldDelimiter(',')
                                .build();
                        hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
                        setFullColumns();
                    } catch (Exception e) {
                        throw new RuntimeException("init client failed", e);
                    }
                    return null;
                }
            });
            return;
        }
        orcSerde = new OrcSerde();
        outputFormat = new org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat();
        jobConf = new JobConf(conf);


        FileOutputFormat.setOutputCompressorClass(jobConf, getCompressType());

        List<ObjectInspector> fullColTypeList = new ArrayList<>();
        decimalColInfo = new HashMap<>((fullColumnTypes.size() << 2) / 3);
        for (int i = 0; i < fullColumnTypes.size(); i++) {
            String columnType = fullColumnTypes.get(i);

            if (ColumnTypeUtil.isDecimalType(columnType)) {
                ColumnTypeUtil.DecimalInfo decimalInfo = ColumnTypeUtil.getDecimalInfo(columnType, ORC_DEFAULT_DECIMAL_INFO);
                decimalColInfo.put(fullColumnNames.get(i), decimalInfo);
            }

            ColumnType type = ColumnType.getType(columnType);
            fullColTypeList.add(HdfsUtil.columnTypeToObjectInspetor(type));
        }
        this.inspector = ObjectInspectorFactory
                .getStandardStructObjectInspector(fullColumnNames, fullColTypeList);
    }

    private Class getCompressType() {
        ECompressType compressType = ECompressType.getByTypeAndFileType(compress, "orc");
        if (ECompressType.ORC_SNAPPY.equals(compressType)) {
            return SnappyCodec.class;
        } else if (ECompressType.ORC_BZIP.equals(compressType)) {
            return BZip2Codec.class;
        } else if (ECompressType.ORC_GZIP.equals(compressType)) {
            return GzipCodec.class;
        } else if (ECompressType.ORC_LZ4.equals(compressType)) {
            return Lz4Codec.class;
        } else {
            return DefaultCodec.class;
        }
    }

    @Override
    protected String getExtension() {
        ECompressType compressType = ECompressType.getByTypeAndFileType(compress, "orc");
        return compressType.getSuffix();
    }

    @Override
    public float getDeviation() {
        ECompressType compressType = ECompressType.getByTypeAndFileType(compress, "orc");
        return compressType.getDeviation();
    }

    @Override
    protected void nextBlock() {
        if (isTransaction) {
            return;
        }
        super.nextBlock();

        if (recordWriter != null) {
            return;
        }

        try {
            String currentBlockTmpPath = tmpPath + SP + currentBlockFileName;
            recordWriter = outputFormat.getRecordWriter(null, jobConf, currentBlockTmpPath, Reporter.NULL);
            blockIndex++;

            setFs();
            LOG.info("nextBlock:Current block writer record:" + rowsOfCurrentBlock);
            LOG.info("Current block file name:" + currentBlockTmpPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeSingleRecordToFile(Row row) throws WriteRecordException {
        if (isTransaction) {
            ugi.doAs(new PrivilegedAction<Void>() {
                public Void run() {
                    try {
                        if (connection == null) {
                            initConnection();
                            connection.beginTransaction();
                        }
                        connection.write(getDataFromRow(row).getBytes());
                    } catch (Exception e) {
                        throw new RuntimeException("WRITER DATA ERROR", e);
                    }
                    return null;
                }
            });
            return;
        }
        if (recordWriter == null) {
            nextBlock();
        }

        List<Object> recordList = new ArrayList<>();
        int i = 0;
        try {
            for (; i < fullColumnNames.size(); ++i) {
                getData(recordList, i, row);
            }
        } catch (Exception e) {
            if (e instanceof WriteRecordException) {
                throw (WriteRecordException) e;
            } else {
                throw new WriteRecordException(recordConvertDetailErrorMessage(i, row), e, i, row);
            }
        }

        try {
            this.recordWriter.write(NullWritable.get(), this.orcSerde.serialize(recordList, this.inspector));
            rowsOfCurrentBlock++;

            if (restoreConfig.isRestore()) {
                lastRow = row;
            }
        } catch (IOException e) {
            throw new WriteRecordException(String.format("数据写入hdfs异常，row:{%s}", row), e);
        }
    }

    private String getDataFromRow(Row row) {
        int length = fullColumnNames.size();
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < length; i++) {
            if (findColumnIndex(fullColumnNames.get(i)) != -1) {
                if (row.getField(i) == null) {
                    str.append(COLUMN_FIELD_DELIMITER);
                    continue;
                }
                str.append(row.getField(i)).append(COLUMN_FIELD_DELIMITER);
                continue;
            }
            str.append(COLUMN_FIELD_DELIMITER);
        }
        return str.toString();
    }

    private int findColumnIndex(String columnName) {
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public void flushDataInternal() throws IOException {
        LOG.info("Close current orc record writer, write data size:[{}]", bytesWriteCounter.getLocalValue());

        if (isTransaction) {
            ugi.doAs(new PrivilegedAction<Void>() {
                public Void run() {
                    try {
                        connection.commitTransaction();
                    } catch (Exception e) {
                        LOG.error("flush data error", e);
                    } finally {
                        connection.close();
                        connection = null;
                    }
                    return null;
                }
            });
            return;
        }
        if (recordWriter != null) {
            recordWriter.close(Reporter.NULL);
            recordWriter = null;
        }
    }

    private void getData(List<Object> recordList, int index, Row row) throws WriteRecordException {
        int j = colIndices[index];
        if (j == -1) {
            recordList.add(null);
            return;
        }

        Object column = row.getField(j);
        if (column == null) {
            recordList.add(null);
            return;
        }

        ColumnType columnType = ColumnType.fromString(columnTypes.get(j));
        String rowData = column.toString();
        if (rowData == null || (rowData.length() == 0 && !ColumnType.isStringType(columnType))) {
            recordList.add(null);
            return;
        }

        switch (columnType) {
            case TINYINT:
                recordList.add(new ByteWritable(Byte.valueOf(rowData)));
                break;
            case SMALLINT:
                recordList.add(new ShortWritable(Short.valueOf(rowData)));
                break;
            case INT:
                recordList.add(Integer.valueOf(rowData));
                break;
            case BIGINT:
                recordList.add(getBigint(column, rowData));
                break;
            case FLOAT:
                recordList.add(Float.valueOf(rowData));
                break;
            case DOUBLE:
                recordList.add(new DoubleWritable(Double.valueOf(rowData)));
                break;
            case DECIMAL:
                recordList.add(getDecimalWritable(index, rowData));
                break;
            case STRING:
                if (column instanceof Timestamp){
                    SimpleDateFormat fm = DateUtil.getDateTimeFormatterForMillisencond();
                    recordList.add(fm.format(column));
                }else if (column instanceof Map || column instanceof List){
                    recordList.add(gson.toJson(column));
                }else {
                    recordList.add(rowData);
                }
                break;
            case VARCHAR:
                HiveVarchar hiveVarchar = new HiveVarchar(rowData, getVarcharLength(columnTypes.get(j)));
                HiveVarcharWritable hiveVarcharWritable = new HiveVarcharWritable(hiveVarchar);
                recordList.add(hiveVarcharWritable);
            case VARCHAR2:
                recordList.add(HiveVarchar2Writable.createInstance(Dialect.UNKNOWN, getVarcharLength(columnTypes.get(j)), rowData));
                break;
            case BOOLEAN:
                recordList.add(Boolean.valueOf(rowData));
                break;
            case DATE:
                java.sql.Date date = DateUtil.columnToDate(column, null);
                recordList.add(new DateWritable(date.getTime()));
                break;
            case TIMESTAMP:
                recordList.add(DateUtil.columnToTimestamp(column, null));
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    private int getVarcharLength(String columnName) {
        if (columnName.contains(ConstantValue.LEFT_PARENTHESIS_SYMBOL)) {
            return Integer.parseInt(columnName.substring(columnName.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL)+1, columnName.indexOf(ConstantValue.RIGHT_PARENTHESIS_SYMBOL)));
        }
        return -1;
    }

    private Object getBigint(Object column, String rowData) {
        if (column instanceof Timestamp) {
            column = ((Timestamp) column).getTime();
            return column;
        }

        BigInteger data = new BigInteger(rowData);
        if (data.compareTo(new BigInteger(String.valueOf(Long.MAX_VALUE))) > 0) {
            return data;
        } else {
            return Long.valueOf(rowData);
        }
    }

    private HiveDecimalWritable getDecimalWritable(int index, String rowData) throws WriteRecordException {
        ColumnTypeUtil.DecimalInfo decimalInfo = decimalColInfo.get(fullColumnNames.get(index));
        HiveDecimal hiveDecimal = HiveDecimal.create(new BigDecimal(rowData));
        hiveDecimal = HiveDecimal.enforcePrecisionScale(hiveDecimal, decimalInfo.getPrecision(), decimalInfo.getScale());
        if(hiveDecimal == null){
            String msg = String.format("第[%s]个数据数据[%s]precision和scale和元数据不匹配:decimal(%s, %s)", index, decimalInfo.getPrecision(), decimalInfo.getScale(), rowData);
            throw new WriteRecordException(msg, new IllegalArgumentException());
        }

        return new HiveDecimalWritable(hiveDecimal);
    }

    @Override
    protected String recordConvertDetailErrorMessage(int pos, Row row) {
        return "\nHdfsOrcOutputFormat [" + jobName + "] writeRecord error: when converting field[" + fullColumnNames.get(pos) + "] in Row(" + row + ")";
    }

    @Override
    protected void closeSource() throws IOException {
        if (isTransaction) {
            ugi.doAs(new PrivilegedAction<Void>() {
                public Void run() {
                    try {
                        connection.commitTransaction();
                    } catch (Exception e) {

                    } finally {
                        connection.close();
                        connection = null;
                    }
                    return null;
                }
            });
            return;
        }
        RecordWriter rw = this.recordWriter;
        if (rw != null) {
            LOG.info("close:Current block writer record:" + rowsOfCurrentBlock);
            rw.close(Reporter.NULL);
            this.recordWriter = null;
        }
    }

    /**
     * 数据源开启kerberos时
     * 如果这里不通过反射对 writerOptions 赋值fs，则在recordWriter.writer时 会初始化一个fs 此fs不在ugi里获取的
     * 导致开启了kerberos的数据源在checkpoint时进行 recordWriter.close() 操作，会出现kerberos认证错误
     *
     * @throws IllegalAccessException
     */
    private void setFs() throws IllegalAccessException {
        Field declaredField = ReflectionUtils.getDeclaredField(recordWriter, "options");
        assert declaredField != null;
        declaredField.setAccessible(true);
        OrcFile.WriterOptions writerOptions = (OrcFile.WriterOptions) declaredField.get(recordWriter);
        writerOptions.fileSystem(fs);
        declaredField.setAccessible(false);
    }
}

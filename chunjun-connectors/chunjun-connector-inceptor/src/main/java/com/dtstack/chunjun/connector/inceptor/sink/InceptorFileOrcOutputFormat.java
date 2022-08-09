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

package com.dtstack.chunjun.connector.inceptor.sink;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.inceptor.converter.InceptorOrcColumnConvent;
import com.dtstack.chunjun.connector.inceptor.enums.ECompressType;
import com.dtstack.chunjun.connector.inceptor.util.InceptorUtil;
import com.dtstack.chunjun.enums.ColumnType;
import com.dtstack.chunjun.security.KerberosUtil;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.WriteRecordException;
import com.dtstack.chunjun.util.ColumnTypeUtil;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.ReflectionUtils;

import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
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
import org.apache.inceptor.streaming.HiveStreamingConnection;
import org.apache.inceptor.streaming.StreamingException;
import org.apache.inceptor.streaming.StrictDelimitedInputWriter;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static com.dtstack.chunjun.connector.inceptor.util.InceptorDbUtil.KEY_PRINCIPAL;

public class InceptorFileOrcOutputFormat extends BaseInceptorFileOutputFormat {

    private static ColumnTypeUtil.DecimalInfo ORC_DEFAULT_DECIMAL_INFO =
            new ColumnTypeUtil.DecimalInfo(
                    HiveDecimal.SYSTEM_DEFAULT_PRECISION, HiveDecimal.SYSTEM_DEFAULT_SCALE);
    private RecordWriter recordWriter;
    private OrcSerde orcSerde;
    private StructObjectInspector inspector;
    private FileOutputFormat outputFormat;
    private JobConf jobConf;
    private StrictDelimitedInputWriter wr;
    private HiveConf hiveConf;
    private HiveMetaStoreClient hiveMetaStoreClient;
    private HiveStreamingConnection connection;

    protected List<String> columnNames;

    @Override
    protected void initVariableFields() {
        columnNames =
                inceptorFileConf.getColumn().stream()
                        .map(FieldConf::getName)
                        .collect(Collectors.toList());

        super.initVariableFields();
    }

    @Override
    protected void openSource() {
        super.openSource();
        if (inceptorFileConf.isTransaction()) {
            ugi.doAs(
                    new PrivilegedAction<Void>() {
                        public Void run() {
                            try {
                                hiveConf = new HiveConf();
                                hiveConf.addResource(conf);
                                if (openKerberos) {
                                    setMetaStoreKerberosConf();
                                }
                                wr =
                                        StrictDelimitedInputWriter.newBuilder()
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

        FileOutputFormat.setOutputCompressorClass(jobConf, getOrcCompressType());

        List<ObjectInspector> fullColTypeList = new ArrayList<>();
        decimalColInfo = new HashMap<>((fullColumnTypeList.size() << 2) / 3);
        for (int i = 0; i < fullColumnTypeList.size(); i++) {
            String columnType = fullColumnTypeList.get(i);

            if (ColumnTypeUtil.isDecimalType(columnType)) {
                ColumnTypeUtil.DecimalInfo decimalInfo =
                        ColumnTypeUtil.getDecimalInfo(columnType, ORC_DEFAULT_DECIMAL_INFO);
                decimalColInfo.put(fullColumnNameList.get(i), decimalInfo);
            }

            ColumnType type = ColumnType.getType(columnType);
            fullColTypeList.add(InceptorUtil.columnTypeToObjectInspetor(type));
        }

        if (rowConverter instanceof InceptorOrcColumnConvent) {
            ((InceptorOrcColumnConvent) rowConverter).setDecimalColInfo(decimalColInfo);
            ((InceptorOrcColumnConvent) rowConverter)
                    .setColumnNameList(
                            inceptorFileConf.getColumn().stream()
                                    .map(FieldConf::getName)
                                    .collect(Collectors.toList()));
        }

        this.inspector =
                ObjectInspectorFactory.getStandardStructObjectInspector(
                        fullColumnNameList, fullColTypeList);
    }

    @Override
    protected void nextBlock() {
        if (inceptorFileConf.isTransaction()) {
            return;
        }

        super.nextBlock();

        if (recordWriter != null) {
            return;
        }

        try {
            String currentBlockTmpPath = tmpPath + File.separatorChar + currentFileName;
            recordWriter =
                    outputFormat.getRecordWriter(null, jobConf, currentBlockTmpPath, Reporter.NULL);
            currentFileIndex++;

            setFs();
            LOG.info("nextBlock:Current block writer record:" + rowsOfCurrentBlock);
            LOG.info("Current block file name:" + currentBlockTmpPath);
        } catch (IOException | IllegalAccessException e) {
            throw new ChunJunRuntimeException(
                    InceptorUtil.parseErrorMsg(null, ExceptionUtil.getErrorMessage(e)), e);
        }
    }

    @Override
    protected void writeSingleRecordToFile(RowData rowData) throws WriteRecordException {

        if (inceptorFileConf.isTransaction()) {
            String rowString;
            try {
                Object[] data = new Object[inceptorFileConf.getColumn().size()];
                try {
                    data = (Object[]) rowConverter.toExternal(rowData, data);
                } catch (Exception e) {
                    String errorMessage =
                            InceptorUtil.parseErrorMsg(
                                    String.format("writer hdfs error，rowData:{%s}", rowData),
                                    ExceptionUtil.getErrorMessage(e));
                    throw new WriteRecordException(errorMessage, e, -1, rowData);
                }
                StringBuilder str = new StringBuilder();
                for (int columnIndex = 0; columnIndex < fullColumnNameList.size(); columnIndex++) {
                    if (columnNames.contains(fullColumnNameList.get(columnIndex))) {
                        if (data[columnIndex] == null) {
                            str.append(",");
                            continue;
                        }

                        str.append(data[columnIndex]).append(",");
                        continue;
                    }
                    str.append(",");
                }
                rowString = str.toString();

            } catch (Exception e) {
                throw new WriteRecordException(String.format("数据写入hdfs异常，row:{%s}", rowData), e);
            }
            ugi.doAs(
                    new PrivilegedAction<Void>() {
                        public Void run() {
                            try {
                                if (connection == null) {
                                    initConnection();
                                    connection.beginTransaction();
                                }
                                connection.write(rowString.getBytes());
                            } catch (Exception e) {
                                throw new RuntimeException("WRITER DATA ERROR", e);
                            }
                            return null;
                        }
                    });
            rowsOfCurrentBlock++;
            lastRow = rowData;
            return;
        }

        if (recordWriter == null) {
            nextBlock();
        }

        Object[] data = new Object[inceptorFileConf.getColumn().size()];
        try {
            data = (Object[]) rowConverter.toExternal(rowData, data);
        } catch (Exception e) {
            String errorMessage =
                    InceptorUtil.parseErrorMsg(
                            String.format("writer hdfs error，rowData:{%s}", rowData),
                            ExceptionUtil.getErrorMessage(e));
            throw new WriteRecordException(errorMessage, e, -1, rowData);
        }

        try {
            this.recordWriter.write(
                    NullWritable.get(), this.orcSerde.serialize(data, this.inspector));
            rowsOfCurrentBlock++;
            lastRow = rowData;
        } catch (IOException e) {
            throw new WriteRecordException(
                    String.format("Data writing to hdfs is abnormal，rowData:{%s}", rowData), e);
        }
    }

    @Override
    protected void flushDataInternal() {
        LOG.info(
                "Close current orc record writer, write data size:[{}]",
                bytesWriteCounter.getLocalValue());
        if (inceptorFileConf.isTransaction()) {
            if (connection == null) {
                return;
            }
            ugi.doAs(
                    new PrivilegedAction<Void>() {
                        public Void run() {
                            try {
                                connection.commitTransaction();
                                connection.close();
                                connection = null;
                            } catch (Exception e) {
                                LOG.error("flush data error", e);
                            }
                            return null;
                        }
                    });
            return;
        }
        try {
            if (recordWriter != null) {
                recordWriter.close(Reporter.NULL);
                recordWriter = null;
            }
        } catch (IOException e) {
            throw new ChunJunRuntimeException(
                    InceptorUtil.parseErrorMsg(
                            "error to flush stream.", ExceptionUtil.getErrorMessage(e)),
                    e);
        }
    }

    @Override
    protected ECompressType getCompressType() {
        return ECompressType.getByTypeAndFileType(inceptorFileConf.getCompress(), "ORC");
    }

    private void setMetaStoreKerberosConf() {
        String keytabFileName =
                KerberosUtil.getPrincipalFileName(inceptorFileConf.getHadoopConfig());
        keytabFileName =
                KerberosUtil.loadFile(
                        inceptorFileConf.getHadoopConfig(), keytabFileName, getDistributedCache());
        String principal =
                inceptorFileConf.getHadoopConfig().get(KEY_PRINCIPAL) == null
                        ? null
                        : inceptorFileConf.getHadoopConfig().get(KEY_PRINCIPAL).toString();
        hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, principal);
        hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_KEYTAB_FILE, keytabFileName);
        hiveConf.setVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, "true");
    }

    private void setFullColumns() throws Exception {
        fullColumnNameList.clear();
        fullColumnTypeList.clear();
        Table inceptorTable =
                hiveMetaStoreClient.getTable(
                        inceptorFileConf.getSchema(), inceptorFileConf.getTable());
        List<FieldSchema> cols = inceptorTable.getSd().getCols();
        if (cols != null && cols.size() > 0) {
            cols.forEach(
                    fieldSchema -> {
                        fullColumnNameList.add(fieldSchema.getName());
                        fullColumnTypeList.add(fieldSchema.getType());
                    });
        }
    }

    private Class getOrcCompressType() {
        ECompressType compressType =
                ECompressType.getByTypeAndFileType(inceptorFileConf.getCompress(), "orc");
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

    /**
     * 数据源开启kerberos时 如果这里不通过反射对 writerOptions 赋值fs，则在recordWriter.writer时 会初始化一个fs 此fs不在ugi里获取的
     * 导致开启了kerberos的数据源在checkpoint时进行 recordWriter.close() 操作，会出现kerberos认证错误
     *
     * @throws IllegalAccessException
     */
    private void setFs() throws IllegalAccessException {
        Field declaredField = ReflectionUtils.getDeclaredField(recordWriter, "options");
        assert declaredField != null;
        declaredField.setAccessible(true);
        OrcFile.WriterOptions writerOptions =
                (OrcFile.WriterOptions) declaredField.get(recordWriter);
        writerOptions.fileSystem(fs);
        declaredField.setAccessible(false);
    }

    private void initConnection() throws StreamingException {
        HiveStreamingConnection.Builder builder =
                HiveStreamingConnection.newBuilder()
                        .withDatabase(inceptorFileConf.getSchema())
                        .withTable(inceptorFileConf.getTable())
                        .withAgentInfo("UT_" + Thread.currentThread().getName())
                        .withTransactionBatchSize(1)
                        .withRecordWriter(wr)
                        .withHiveConf(hiveConf)
                        .withClient(hiveMetaStoreClient);
        if (StringUtils.isNotEmpty(inceptorFileConf.getPartitionName())) {
            List<String> partitions = Arrays.asList(inceptorFileConf.getPartitionName());
            builder.withStaticPartitionValues(partitions);
        }
        connection = builder.connect();
    }
}

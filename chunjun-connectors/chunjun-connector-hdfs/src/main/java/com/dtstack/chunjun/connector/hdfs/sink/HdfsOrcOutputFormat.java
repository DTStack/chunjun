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
package com.dtstack.chunjun.connector.hdfs.sink;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.hdfs.converter.HdfsOrcSyncConverter;
import com.dtstack.chunjun.connector.hdfs.enums.CompressType;
import com.dtstack.chunjun.connector.hdfs.enums.FileType;
import com.dtstack.chunjun.connector.hdfs.util.HdfsUtil;
import com.dtstack.chunjun.enums.ColumnType;
import com.dtstack.chunjun.enums.SizeUnitType;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.WriteRecordException;
import com.dtstack.chunjun.util.ColumnTypeUtil;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.FileSystemUtil;
import com.dtstack.chunjun.util.ReflectionUtils;

import org.apache.flink.table.data.RowData;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class HdfsOrcOutputFormat extends BaseHdfsOutputFormat {
    private static final long serialVersionUID = -7422373310272271581L;

    private static final ColumnTypeUtil.DecimalInfo ORC_DEFAULT_DECIMAL_INFO =
            new ColumnTypeUtil.DecimalInfo(
                    HiveDecimal.SYSTEM_DEFAULT_PRECISION, HiveDecimal.SYSTEM_DEFAULT_SCALE);
    private RecordWriter recordWriter;
    private OrcSerde orcSerde;
    private StructObjectInspector inspector;
    private FileOutputFormat outputFormat;
    private JobConf jobConfig;

    protected int[] colIndices;

    @Override
    protected void openSource() {
        super.openSource();

        orcSerde = new OrcSerde();
        outputFormat = new org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat();
        jobConfig = new JobConf(config);

        Class<? extends CompressionCodec> codecClass;
        switch (compressType) {
            case ORC_SNAPPY:
                codecClass = SnappyCodec.class;
                break;
            case ORC_BZIP:
                codecClass = BZip2Codec.class;
                break;
            case ORC_GZIP:
                codecClass = GzipCodec.class;
                break;
            case ORC_LZ4:
                codecClass = Lz4Codec.class;
                break;
            default:
                codecClass = DefaultCodec.class;
        }
        FileOutputFormat.setOutputCompressorClass(jobConfig, codecClass);

        int size = hdfsConfig.getFullColumnType().size();
        decimalColInfo = Maps.newHashMapWithExpectedSize(size);
        List<ObjectInspector> structFieldObjectInspectors = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            String columnType = hdfsConfig.getFullColumnType().get(i);

            if (ColumnTypeUtil.isDecimalType(columnType)) {
                ColumnTypeUtil.DecimalInfo decimalInfo =
                        ColumnTypeUtil.getDecimalInfo(columnType, ORC_DEFAULT_DECIMAL_INFO);
                decimalColInfo.put(hdfsConfig.getFullColumnName().get(i), decimalInfo);
            }
            ColumnType type = ColumnType.getType(columnType);
            structFieldObjectInspectors.add(HdfsUtil.columnTypeToObjectInspetor(type));
        }

        if (rowConverter instanceof HdfsOrcSyncConverter) {
            ((HdfsOrcSyncConverter) rowConverter).setDecimalColInfo(decimalColInfo);
            ((HdfsOrcSyncConverter) rowConverter)
                    .setColumnNameList(
                            hdfsConfig.getColumn().stream()
                                    .map(FieldConfig::getName)
                                    .collect(Collectors.toList()));
        }
        this.inspector =
                ObjectInspectorFactory.getStandardStructObjectInspector(
                        fullColumnNameList, structFieldObjectInspectors);

        colIndices = new int[hdfsConfig.getFullColumnName().size()];
        for (int i = 0; i < hdfsConfig.getFullColumnName().size(); ++i) {
            int j = 0;
            for (; j < hdfsConfig.getColumn().size(); ++j) {
                if (hdfsConfig
                        .getFullColumnName()
                        .get(i)
                        .equalsIgnoreCase(hdfsConfig.getColumn().get(j).getName())) {
                    colIndices[i] = j;
                    break;
                }
            }
            if (j == hdfsConfig.getColumn().size()) {
                colIndices[i] = -1;
            }
        }
    }

    @Override
    // todo the deviation needs to be calculated accurately
    protected long getCurrentFileSize() {
        return (long) (bytesWriteCounter.getLocalValue() * getDeviation());
    }

    @Override
    protected void nextBlock() {
        super.nextBlock();

        if (recordWriter != null) {
            return;
        }

        try {
            String currentBlockTmpPath = tmpPath + getHdfsPathChar() + currentFileName;
            recordWriter =
                    outputFormat.getRecordWriter(
                            null, jobConfig, currentBlockTmpPath, Reporter.NULL);
            currentFileIndex++;

            setFs();
            log.info("nextBlock:Current block writer record:" + rowsOfCurrentBlock);
            log.info("Current block file name:" + currentBlockTmpPath);
        } catch (IOException | IllegalAccessException e) {
            throw new ChunJunRuntimeException(
                    HdfsUtil.parseErrorMsg(null, ExceptionUtil.getErrorMessage(e)), e);
        }
    }

    @Override
    public void flushDataInternal() {
        log.info(
                "Close current orc record writer, write data size:[{}]",
                SizeUnitType.readableFileSize(bytesWriteCounter.getLocalValue()));

        try {
            if (recordWriter != null) {
                recordWriter.close(Reporter.NULL);
                recordWriter = null;
            }
        } catch (IOException e) {
            throw new ChunJunRuntimeException(
                    HdfsUtil.parseErrorMsg(
                            "error to flush stream.", ExceptionUtil.getErrorMessage(e)),
                    e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeSingleRecordToFile(RowData rowData) throws WriteRecordException {
        if (recordWriter == null) {
            nextBlock();
        }

        Object[] data = new Object[hdfsConfig.getColumn().size()];
        try {
            data = (Object[]) rowConverter.toExternal(rowData, data);
        } catch (Exception e) {
            String errorMessage =
                    HdfsUtil.parseErrorMsg(
                            String.format("writer hdfs error，rowData:{%s}", rowData),
                            ExceptionUtil.getErrorMessage(e));
            throw new WriteRecordException(errorMessage, e, -1, rowData);
        }

        try {
            List<Object> recordList = new ArrayList<>();
            for (int i = 0; i < hdfsConfig.getFullColumnName().size(); ++i) {
                int colIndex = colIndices[i];
                if (colIndex == -1) {
                    recordList.add(null);
                } else {
                    recordList.add(data[colIndex]);
                }
            }

            this.recordWriter.write(
                    NullWritable.get(), this.orcSerde.serialize(recordList, this.inspector));
            rowsOfCurrentBlock++;
            lastRow = rowData;
        } catch (IOException e) {
            throw new WriteRecordException(
                    String.format("Data writing to hdfs is abnormal，rowData:{%s}", rowData), e);
        }
    }

    @Override
    protected void closeSource() {
        try {
            log.info("close:Current block writer record:" + rowsOfCurrentBlock);
            RecordWriter rw = this.recordWriter;
            if (rw != null) {
                rw.close(Reporter.NULL);
                this.recordWriter = null;
            }
        } catch (IOException e) {
            throw new ChunJunRuntimeException("close stream error.", e);
        } finally {
            super.closeSource();
        }
    }

    @Override
    public CompressType getCompressType() {
        return CompressType.getByTypeAndFileType(hdfsConfig.getCompress(), FileType.ORC.name());
    }

    /**
     * 数据源开启kerberos时 如果这里不通过反射对 writerOptions 赋值fs，则在recordWriter.writer时 会初始化一个fs 此fs不在ugi里获取的
     * 导致开启了kerberos的数据源在checkpoint时进行 recordWriter.close() 操作，会出现kerberos认证错误
     *
     * @throws IllegalAccessException illegal access exception.
     */
    private void setFs() throws IllegalAccessException {
        if (FileSystemUtil.isOpenKerberos(hdfsConfig.getHadoopConfig())) {
            Field declaredField = ReflectionUtils.getDeclaredField(recordWriter, "options");
            assert declaredField != null;
            declaredField.setAccessible(true);
            OrcFile.WriterOptions writerOptions =
                    (OrcFile.WriterOptions) declaredField.get(recordWriter);
            writerOptions.fileSystem(fs);
            declaredField.setAccessible(false);
        }
    }
}

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

package com.dtstack.chunjun.connector.hive3.sink;

import com.dtstack.chunjun.connector.hive3.enums.CompressType;
import com.dtstack.chunjun.connector.hive3.enums.FileType;
import com.dtstack.chunjun.connector.hive3.util.Hive3Util;
import com.dtstack.chunjun.enums.ColumnType;
import com.dtstack.chunjun.enums.SizeUnitType;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.WriteRecordException;
import com.dtstack.chunjun.util.ColumnTypeUtil;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.ReflectionUtils;

import org.apache.flink.table.data.RowData;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.common.type.HiveDecimal;
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
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Slf4j
public class HdfsOrcOutputFormat extends BaseHdfsOutputFormat {
    private static final long serialVersionUID = 1528560024520596147L;

    private RecordWriter recordWriter;
    private OrcSerde orcSerde;
    private StructObjectInspector inspector;
    private FileOutputFormat outputFormat;
    private JobConf jobConf;
    private static final ColumnTypeUtil.DecimalInfo ORC_DEFAULT_DECIMAL_INFO =
            new ColumnTypeUtil.DecimalInfo(
                    HiveDecimal.SYSTEM_DEFAULT_PRECISION, HiveDecimal.SYSTEM_DEFAULT_SCALE);

    // kerberos 认证
    protected transient UserGroupInformation ugi;
    protected boolean openKerberos;
    public static final String HADOOP_USER_NAME = "hadoop.user.name";
    public static final String KEY_PRINCIPAL = "principal";

    @Override
    protected void openSource() {
        super.openSource();
        orcSerde = new OrcSerde();
        outputFormat = new org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat();
        jobConf = new JobConf(conf);
        FileOutputFormat.setOutputCompressorClass(jobConf, getOrcCodecClass(compressType));

        List<ObjectInspector> fullColTypeList = new ArrayList<>();

        decimalColInfo = new HashMap<>((fullColumnTypeList.size() << 2) / 3);
        for (int i = 0; i < fullColumnTypeList.size(); i++) {
            String columnType = fullColumnTypeList.get(i);

            ColumnTypeUtil.DecimalInfo decimalInfo = null;
            if (ColumnTypeUtil.isDecimalType(columnType)) {
                decimalInfo = ColumnTypeUtil.getDecimalInfo(columnType, ORC_DEFAULT_DECIMAL_INFO);
                decimalColInfo.put(fullColumnNameList.get(i), decimalInfo);
            }

            ColumnType type = ColumnType.getType(columnType);
            fullColTypeList.add(Hive3Util.columnTypeToObjectInspector(type, decimalInfo));
        }

        this.inspector =
                ObjectInspectorFactory.getStandardStructObjectInspector(
                        fullColumnNameList, fullColTypeList);
    }

    @Override
    protected CompressType getCompressType() {
        return CompressType.getByTypeAndFileType(hdfsConfig.getCompress(), FileType.ORC.name());
    }

    protected Class getOrcCodecClass(CompressType compressType) {
        if (CompressType.ORC_SNAPPY.equals(compressType)) {
            return SnappyCodec.class;
        } else if (CompressType.ORC_BZIP.equals(compressType)) {
            return BZip2Codec.class;
        } else if (CompressType.ORC_GZIP.equals(compressType)) {
            return GzipCodec.class;
        } else if (CompressType.ORC_LZ4.equals(compressType)) {
            return Lz4Codec.class;
        } else {
            return DefaultCodec.class;
        }
    }

    @Override
    protected void nextBlock() {
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
            log.info("nextBlock:Current block writer record:" + rowsOfCurrentBlock);
            log.info("Current block file name:" + currentBlockTmpPath);
        } catch (Exception e) {
            throw new RuntimeException(
                    Hive3Util.parseErrorMsg(null, ExceptionUtil.getErrorMessage(e)), e);
        }
    }

    /**
     * 数据源开启kerberos时 如果这里不通过反射对 writerOptions 赋值fs，则在recordWriter.writer时 会初始化一个fs 此fs不在ugi里获取的
     * 导致开启了kerberos的数据源在checkpoint时进行 recordWriter.close() 操作，会出现kerberos认证错误
     */
    private void setFs() throws IllegalAccessException {
        if (Hive3Util.isOpenKerberos(hdfsConfig.getHadoopConfig())) {
            Field declaredField = ReflectionUtils.getDeclaredField(recordWriter, "options");
            assert declaredField != null;
            declaredField.setAccessible(true);
            OrcFile.WriterOptions writerOptions =
                    (OrcFile.WriterOptions) declaredField.get(recordWriter);
            writerOptions.fileSystem(fs);
            declaredField.setAccessible(false);
        }
    }

    @Override
    public void writeSingleRecordToFile(RowData row) throws WriteRecordException {
        if (recordWriter == null) {
            nextBlock();
        }

        List<Object> recordList = new ArrayList<>();
        int i = 0;
        try {
            rowConverter.toExternal(row, recordList);
        } catch (Exception e) {
            if (e instanceof WriteRecordException) {
                throw (WriteRecordException) e;
            } else {
                throw new WriteRecordException(recordConvertDetailErrorMessage(i, row), e, i, row);
            }
        }

        try {
            this.recordWriter.write(
                    NullWritable.get(), this.orcSerde.serialize(recordList, this.inspector));
            rowsOfCurrentBlock++;

            lastRow = row;
        } catch (IOException e) {
            String errorMessage =
                    Hive3Util.parseErrorMsg(
                            String.format("writer hdfs error，row:{%s}", row),
                            ExceptionUtil.getErrorMessage(e));
            log.error(errorMessage);
            throw new WriteRecordException(errorMessage, e);
        }
    }

    @Override
    // todo the deviation needs to be calculated accurately
    protected long getCurrentFileSize() {
        return (long) (bytesWriteCounter.getLocalValue() * getDeviation());
    }

    @Override
    protected void flushDataInternal() {
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
                    Hive3Util.parseErrorMsg(
                            "error to flush stream.", ExceptionUtil.getErrorMessage(e)),
                    e);
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
}

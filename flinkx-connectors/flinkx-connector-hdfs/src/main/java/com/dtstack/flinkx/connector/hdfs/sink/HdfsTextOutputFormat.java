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
package com.dtstack.flinkx.connector.hdfs.sink;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.hdfs.enums.CompressType;
import com.dtstack.flinkx.connector.hdfs.enums.FileType;
import com.dtstack.flinkx.connector.hdfs.util.HdfsUtil;
import com.dtstack.flinkx.enums.SizeUnitType;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.throwable.WriteRecordException;
import com.dtstack.flinkx.util.ExceptionUtil;

import org.apache.flink.table.data.RowData;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Date: 2021/06/09 Company: www.dtstack.com
 *
 * @author tudou
 */
public class HdfsTextOutputFormat extends BaseHdfsOutputFormat {

    private static final int NEWLINE = 10;
    private transient OutputStream stream;

    @Override
    protected void nextBlock() {
        super.nextBlock();

        if (stream != null) {
            return;
        }

        try {
            String currentBlockTmpPath = tmpPath + File.separatorChar + currentFileName;
            Path p = new Path(currentBlockTmpPath);

            if (CompressType.TEXT_NONE.equals(compressType)) {
                stream = fs.create(p);
            } else {
                p = new Path(currentBlockTmpPath);
                if (compressType == CompressType.TEXT_GZIP) {
                    stream = new GzipCompressorOutputStream(fs.create(p));
                } else if (compressType == CompressType.TEXT_BZIP2) {
                    stream = new BZip2CompressorOutputStream(fs.create(p));
                }
            }
            currentFileIndex++;
            LOG.info("subtask:[{}] create block file:{}", taskNumber, currentBlockTmpPath);
        } catch (IOException e) {
            throw new FlinkxRuntimeException(
                    HdfsUtil.parseErrorMsg(null, ExceptionUtil.getErrorMessage(e)), e);
        }
    }

    @Override
    public void flushDataInternal() {
        LOG.info(
                "Close current text stream, write data size:[{}]",
                SizeUnitType.readableFileSize(bytesWriteCounter.getLocalValue()));

        try {
            if (stream != null) {
                stream.flush();
                stream.close();
                stream = null;
            }
        } catch (IOException e) {
            throw new FlinkxRuntimeException(
                    HdfsUtil.parseErrorMsg(
                            "error to flush stream.", ExceptionUtil.getErrorMessage(e)),
                    e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeSingleRecordToFile(RowData rowData) throws WriteRecordException {
        if (stream == null) {
            nextBlock();
        }
        String[] data = new String[hdfsConf.getColumn().size()];
        try {
            data = (String[]) rowConverter.toExternal(rowData, data);
        } catch (Exception e) {
            throw new WriteRecordException("can't parse rowData", e, -1, rowData);
        }

        String[] result = new String[fullColumnNameList.size()];
        for (int i = 0; i < hdfsConf.getColumn().size(); i++) {
            FieldConf fieldConf = hdfsConf.getColumn().get(i);
            result[fieldConf.getIndex()] = data[i];
        }
        String line = String.join(hdfsConf.getFieldDelimiter(), result);

        try {
            byte[] bytes = line.getBytes(hdfsConf.getEncoding());
            this.stream.write(bytes);
            this.stream.write(NEWLINE);
            rowsOfCurrentBlock++;
            lastRow = rowData;
        } catch (IOException e) {
            String errorMessage =
                    HdfsUtil.parseErrorMsg(
                            String.format("writer hdfs error，rowData:{%s}", rowData),
                            ExceptionUtil.getErrorMessage(e));
            throw new WriteRecordException(errorMessage, e, -1, rowData);
        }
    }

    @Override
    public void closeSource() {
        try {
            OutputStream outputStream = this.stream;
            if (outputStream != null) {
                outputStream.flush();
                this.stream = null;
                outputStream.close();
            }
        } catch (IOException e) {
            throw new FlinkxRuntimeException("close stream error.", e);
        } finally {
            super.closeSource();
        }
    }

    @Override
    public CompressType getCompressType() {
        return CompressType.getByTypeAndFileType(hdfsConf.getCompress(), FileType.TEXT.name());
    }
}

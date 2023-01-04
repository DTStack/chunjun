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
import com.dtstack.chunjun.connector.hdfs.enums.CompressType;
import com.dtstack.chunjun.connector.hdfs.enums.FileType;
import com.dtstack.chunjun.connector.hdfs.util.HdfsUtil;
import com.dtstack.chunjun.enums.SizeUnitType;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.WriteRecordException;
import com.dtstack.chunjun.util.ExceptionUtil;

import org.apache.flink.table.data.RowData;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;

@Slf4j
public class HdfsTextOutputFormat extends BaseHdfsOutputFormat {
    private static final long serialVersionUID = 3593076392791803459L;

    private static final int NEWLINE = 10;
    private transient OutputStream stream;

    @Override
    protected void nextBlock() {
        super.nextBlock();

        if (stream != null) {
            return;
        }

        try {
            String currentBlockTmpPath = tmpPath + getHdfsPathChar() + currentFileName;
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
            log.info("subtask:[{}] create block file:{}", taskNumber, currentBlockTmpPath);
        } catch (IOException e) {
            throw new ChunJunRuntimeException(
                    HdfsUtil.parseErrorMsg(null, ExceptionUtil.getErrorMessage(e)), e);
        }
    }

    @Override
    public void flushDataInternal() {
        log.info(
                "Close current text stream, write data size:[{}]",
                SizeUnitType.readableFileSize(bytesWriteCounter.getLocalValue()));

        try {
            if (stream != null) {
                stream.flush();
                stream.close();
                stream = null;
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
        if (stream == null) {
            nextBlock();
        }
        String[] data = new String[hdfsConfig.getColumn().size()];
        try {
            data = (String[]) rowConverter.toExternal(rowData, data);
        } catch (Exception e) {
            throw new WriteRecordException("can't parse rowData", e, -1, rowData);
        }

        String[] result = new String[fullColumnNameList.size()];
        for (int i = 0; i < hdfsConfig.getColumn().size(); i++) {
            FieldConfig fieldConfig = hdfsConfig.getColumn().get(i);
            result[fieldConfig.getIndex()] = data[i];
        }
        String line = String.join(hdfsConfig.getFieldDelimiter(), result);

        try {
            byte[] bytes = line.getBytes(hdfsConfig.getEncoding());
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
            throw new ChunJunRuntimeException("close stream error.", e);
        } finally {
            super.closeSource();
        }
    }

    @Override
    public CompressType getCompressType() {
        return CompressType.getByTypeAndFileType(hdfsConfig.getCompress(), FileType.TEXT.name());
    }
}

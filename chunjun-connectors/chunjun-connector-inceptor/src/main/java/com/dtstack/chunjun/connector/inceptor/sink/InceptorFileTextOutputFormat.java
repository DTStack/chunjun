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
import com.dtstack.chunjun.connector.inceptor.enums.ECompressType;
import com.dtstack.chunjun.connector.inceptor.util.InceptorUtil;
import com.dtstack.chunjun.enums.SizeUnitType;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.WriteRecordException;
import com.dtstack.chunjun.util.ExceptionUtil;

import org.apache.flink.table.data.RowData;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

public class InceptorFileTextOutputFormat extends BaseInceptorFileOutputFormat {

    private static final int NEWLINE = 10;
    private transient OutputStream stream;

    private static final int BUFFER_SIZE = 1000;

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
    protected ECompressType getCompressType() {
        return ECompressType.getByTypeAndFileType(inceptorFileConf.getCompress(), "TEXT");
    }

    @Override
    protected void writeSingleRecordToFile(RowData rowData) throws WriteRecordException {
        if (stream == null) {
            nextBlock();
        }
        String[] data = new String[inceptorFileConf.getColumn().size()];
        try {
            data = (String[]) rowConverter.toExternal(rowData, data);
        } catch (Exception e) {
            throw new WriteRecordException("can't parse rowData", e, -1, rowData);
        }

        String[] result = new String[fullColumnNameList.size()];
        for (int i = 0; i < inceptorFileConf.getColumn().size(); i++) {
            FieldConf fieldConf = inceptorFileConf.getColumn().get(i);
            result[fieldConf.getIndex()] = data[i];
        }
        String line = String.join(inceptorFileConf.getFieldDelimiter(), result);

        try {
            byte[] bytes = line.getBytes(inceptorFileConf.getEncoding());
            this.stream.write(bytes);
            this.stream.write(NEWLINE);
            rowsOfCurrentBlock++;
            lastRow = rowData;
        } catch (IOException e) {
            String errorMessage =
                    InceptorUtil.parseErrorMsg(
                            String.format("writer hdfs error，rowData:{%s}", rowData),
                            ExceptionUtil.getErrorMessage(e));
            throw new WriteRecordException(errorMessage, e, -1, rowData);
        }
    }

    @Override
    protected void flushDataInternal() {
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
            throw new ChunJunRuntimeException(
                    InceptorUtil.parseErrorMsg(
                            "error to flush stream.", ExceptionUtil.getErrorMessage(e)),
                    e);
        }
    }

    @Override
    protected void nextBlock() {
        super.nextBlock();

        if (stream != null) {
            return;
        }

        try {
            String currentBlockTmpPath = tmpPath + File.separatorChar + currentFileName;
            Path p = new Path(currentBlockTmpPath);

            if (ECompressType.TEXT_NONE.equals(compressType)) {
                stream = fs.create(p);
            } else {
                p = new Path(currentBlockTmpPath);
                if (compressType == ECompressType.TEXT_GZIP) {
                    stream = new GzipCompressorOutputStream(fs.create(p));
                } else if (compressType == ECompressType.TEXT_BZIP2) {
                    stream = new BZip2CompressorOutputStream(fs.create(p));
                }
            }
            currentFileIndex++;
            LOG.info("subtask:[{}] create block file:{}", taskNumber, currentBlockTmpPath);
        } catch (IOException e) {
            throw new ChunJunRuntimeException(
                    InceptorUtil.parseErrorMsg(null, ExceptionUtil.getErrorMessage(e)), e);
        }
    }
}

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

package com.dtstack.chunjun.connector.s3.sink;

import com.dtstack.chunjun.config.FieldConf;
import com.dtstack.chunjun.connector.s3.conf.S3Conf;
import com.dtstack.chunjun.connector.s3.util.S3Util;
import com.dtstack.chunjun.connector.s3.util.WriterUtil;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.WriteRecordException;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.RowData;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PartETag;
import com.esotericsoftware.minlog.Log;
import org.apache.commons.lang3.StringUtils;

import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The OutputFormat Implementation which write data to Amazon S3.
 *
 * @author jier
 */
public class S3OutputFormat extends BaseRichOutputFormat {

    private transient AmazonS3 amazonS3;

    private S3Conf s3Conf;

    /** Must start at 1 and cannot be greater than 10,000 */
    private static int currentPartNumber;

    private static String currentUploadId;
    private static boolean willClose = false;
    private transient StringWriter sw;
    private transient List<MyPartETag> myPartETags;

    private static final String OVERWRITE_MODE = "overwrite";
    private transient WriterUtil writerUtil;

    private static final long MIN_SIZE = 1024 * 1024 * 25L;

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        openSource();
        restore();
        checkOutputDir();
        createActionFinishedTag();
        nextBlock();
        List<FieldConf> column = s3Conf.getColumn();
        columnNameList = column.stream().map(FieldConf::getName).collect(Collectors.toList());
        columnTypeList = column.stream().map(FieldConf::getType).collect(Collectors.toList());
    }

    private void openSource() {
        this.amazonS3 = S3Util.getS3Client(s3Conf);
        this.myPartETags = new ArrayList<>();
        this.currentPartNumber = taskNumber - numTasks + 1;
        beforeWriteRecords();
    }

    private void restore() {
        if (formatState != null && formatState.getState() != null) {
            Tuple2<String, List<MyPartETag>> state =
                    (Tuple2<String, List<MyPartETag>>) formatState.getState();
            this.currentUploadId = state.f0;
            this.myPartETags = state.f1;
        }
    }

    private void checkOutputDir() {
        if (S3Util.doesObjectExist(amazonS3, s3Conf.getBucket(), s3Conf.getObject())) {
            if (OVERWRITE_MODE.equalsIgnoreCase(s3Conf.getWriteMode())) {
                S3Util.deleteObject(amazonS3, s3Conf.getBucket(), s3Conf.getObject());
            }
        }
    }

    private void nextBlock() {
        sw = new StringWriter();
        this.writerUtil = new WriterUtil(sw, s3Conf.getFieldDelimiter());
        this.currentPartNumber = this.currentPartNumber + numTasks;
    }

    /** Create file multipart upload ID */
    private void createActionFinishedTag() {
        if (!StringUtils.isNotBlank(currentUploadId)) {
            this.currentUploadId =
                    S3Util.initiateMultipartUploadAndGetId(
                            amazonS3, s3Conf.getBucket(), s3Conf.getObject());
        }
    }

    private void beforeWriteRecords() {
        if (s3Conf.isFirstLineHeader()) {
            try {
                RowData rowData =
                        rowConverter.toInternal(
                                columnNameList.toArray(new String[columnNameList.size()]));
                writeSingleRecordInternal(rowData);
            } catch (Exception e) {
                e.printStackTrace();
                LOG.warn("first line fail to write");
            }
        }
    }

    protected void flushDataInternal() {
        StringBuffer sb = sw.getBuffer();
        if (sb.length() > MIN_SIZE || willClose) {
            byte[] byteArray;
            try {
                byteArray = sb.toString().getBytes(s3Conf.getEncoding());
            } catch (UnsupportedEncodingException e) {
                throw new ChunJunRuntimeException(e);
            }
            LOG.info("Upload part size：" + byteArray.length);
            PartETag partETag =
                    S3Util.uploadPart(
                            amazonS3,
                            s3Conf.getBucket(),
                            s3Conf.getObject(),
                            this.currentUploadId,
                            this.currentPartNumber,
                            byteArray);

            MyPartETag myPartETag = new MyPartETag(partETag);
            myPartETags.add(myPartETag);

            LOG.debug(
                    "task-{} upload etag:[{}]",
                    taskNumber,
                    myPartETags.stream().map(Objects::toString).collect(Collectors.joining(",")));
            writerUtil.close();
            writerUtil = null;
        }
    }

    private void completeMultipartUploadFile() {
        if (this.currentPartNumber > 10000) {
            throw new IllegalArgumentException("part can not bigger than 10000");
        }
        List<PartETag> partETags =
                myPartETags.stream().map(MyPartETag::genPartETag).collect(Collectors.toList());
        if (partETags.size() > 0) {
            LOG.info(
                    "Start merging files partETags:{}",
                    partETags.stream().map(PartETag::getETag).collect(Collectors.joining(",")));
            S3Util.completeMultipartUpload(
                    amazonS3,
                    s3Conf.getBucket(),
                    s3Conf.getObject(),
                    this.currentUploadId,
                    partETags);
        } else {
            S3Util.abortMultipartUpload(
                    amazonS3, s3Conf.getBucket(), s3Conf.getObject(), this.currentUploadId);
            S3Util.putStringObject(amazonS3, s3Conf.getBucket(), s3Conf.getObject(), "");
        }
    }

    @Override
    public void closeInternal() {
        // Before closing the client, upload the remaining data smaller than 5M
        willClose = true;
        flushDataInternal();
        completeMultipartUploadFile();
        S3Util.closeS3(amazonS3);
        Log.info("S3Client close!");
    }

    @Override
    protected void writeMultipleRecordsInternal() {
        throw new UnsupportedOperationException("S3 Writer does not support batch write");
    }

    @Override
    public FormatState getFormatState() throws Exception {
        super.getFormatState();
        if (formatState != null) {
            formatState.setNumOfSubTask(taskNumber);
            formatState.setState(new Tuple2<>(currentUploadId, myPartETags));
        }
        return formatState;
    }

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        try {
            if (this.writerUtil == null) {
                nextBlock();
            }
            String[] stringRecord = new String[columnNameList.size()];
            // convert row to string
            rowConverter.toExternal(rowData, stringRecord);
            try {
                for (int i = 0; i < columnNameList.size(); ++i) {

                    String column = stringRecord[i];

                    if (column == null) {
                        continue;
                    }
                    writerUtil.write(column);
                }
                writerUtil.endRecord();
                flushDataInternal();
            } catch (Exception ex) {
                String msg = "RowData2string error RowData(" + rowData + ")";
                throw new WriteRecordException(msg, ex, 0, rowData);
            }
        } catch (Exception ex) {
            throw new WriteRecordException(ex.getMessage(), ex);
        }
    }

    public void setS3Conf(S3Conf s3Conf) {
        this.s3Conf = s3Conf;
    }
}

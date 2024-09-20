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

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.s3.config.S3Config;
import com.dtstack.chunjun.connector.s3.util.S3Util;
import com.dtstack.chunjun.connector.s3.util.WriterUtil;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.WriteRecordException;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.RowData;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PartETag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.dtstack.chunjun.format.tika.config.TikaReadConfig.ORIGINAL_FILENAME;

/** The OutputFormat Implementation which write data to Amazon S3. */
@Slf4j
public class S3OutputFormat extends BaseRichOutputFormat {

    private static final long serialVersionUID = -7314796548887092597L;

    private transient AmazonS3 amazonS3;

    private S3Config s3Config;

    /** Must start at 1 and cannot be greater than 10,000 */
    private int currentPartNumber;

    private String currentUploadId;
    private boolean willClose = false;
    private transient StringWriter sw;
    private transient List<MyPartETag> myPartETags;

    private static final String OVERWRITE_MODE = "overwrite";
    private transient WriterUtil writerUtil;

    private static final long MIN_SIZE = 1024 * 1024 * 25L;

    @Override
    public void initializeGlobal(int parallelism) {
        this.amazonS3 = S3Util.getS3Client(s3Config);
        if (OVERWRITE_MODE.equalsIgnoreCase(s3Config.getWriteMode())) {
            checkOutputDir();
        }
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        // 写多个对象时
        if (!s3Config.isWriteSingleObject()) {
            s3Config.setObject(
                    s3Config.getObject()
                            + ConstantValue.SINGLE_SLASH_SYMBOL
                            + jobId
                            + "_"
                            + taskNumber
                            + getExtension());
        } else {
            // 写单个对象时
            if (OVERWRITE_MODE.equalsIgnoreCase(s3Config.getWriteMode())) {
                // 当写入模式是overwrite时
                s3Config.setObject(s3Config.getObject() + getExtension());
            } else {
                // 当写入模式是append时
                s3Config.setObject(
                        s3Config.getObject() + "_" + jobId + "_" + taskNumber + getExtension());
            }
        }
        log.info("current write object name: {}", s3Config.getObject());
        List<FieldConfig> column = s3Config.getColumn();
        columnNameList = column.stream().map(FieldConfig::getName).collect(Collectors.toList());
        columnTypeList = column.stream().map(FieldConfig::getType).collect(Collectors.toList());
        openSource();
        restore();
        createActionFinishedTag();
        nextBlock();
    }

    private void openSource() {
        this.amazonS3 = S3Util.getS3Client(s3Config);
        this.myPartETags = new ArrayList<>();
        this.currentPartNumber = 0;
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
        // 覆盖写单个对象时
        if (s3Config.isWriteSingleObject()
                && S3Util.doesObjectExist(amazonS3, s3Config.getBucket(), s3Config.getObject())) {
            S3Util.deleteObject(amazonS3, s3Config.getBucket(), s3Config.getObject());
        }
        // 覆盖写多个对象时
        if (!s3Config.isWriteSingleObject()) {
            List<String> subObjects;
            if (s3Config.isUseV2()) {
                subObjects =
                        S3Util.listObjectsKeyByPrefix(
                                amazonS3,
                                s3Config.getBucket(),
                                s3Config.getObject(),
                                s3Config.getFetchSize(),
                                s3Config.getObjectsRegex());
            } else {
                subObjects =
                        S3Util.listObjectsByv1(
                                amazonS3,
                                s3Config.getBucket(),
                                s3Config.getObject(),
                                s3Config.getFetchSize());
            }
            String[] keys = subObjects.toArray(new String[] {});
            S3Util.deleteObjects(amazonS3, s3Config.getBucket(), keys);
            log.info("delete objects num：" + keys.length);
            log.debug("delete objects list：" + StringUtils.join(keys, ","));
        }
    }

    public String getExtension() {
        if (StringUtils.isNotBlank(s3Config.getSuffix())) {
            return s3Config.getSuffix();
        } else {
            return "";
        }
    }

    private void nextBlock() {
        if (sw == null) {
            sw = new StringWriter();
        }
        this.writerUtil = new WriterUtil(sw, s3Config.getFieldDelimiter());
        if (!s3Config.isUseTextQualifier()) {
            writerUtil.setUseTextQualifier(false);
        }
        this.currentPartNumber = this.currentPartNumber + 1;
    }

    /** Create file multipart upload ID */
    private void createActionFinishedTag() {
        if (s3Config.isEnableWriteSingleRecordAsFile()) {
            return;
        }
        if (!StringUtils.isNotBlank(currentUploadId)) {
            this.currentUploadId =
                    S3Util.initiateMultipartUploadAndGetId(
                            amazonS3, s3Config.getBucket(), s3Config.getObject());
        }
    }

    private void beforeWriteRecords() {
        if (s3Config.isFirstLineHeader()) {
            try {
                RowData rowData =
                        rowConverter.toInternal(
                                columnNameList.toArray(new String[columnNameList.size()]));
                writeSingleRecordInternal(rowData);
            } catch (Exception e) {
                e.printStackTrace();
                log.warn("first line fail to write");
            }
        }
    }

    protected void flushDataInternal() {
        if (sw == null) {
            return;
        }
        StringBuffer sb = sw.getBuffer();
        if (sb.length() > MIN_SIZE || willClose || s3Config.isEnableWriteSingleRecordAsFile()) {
            byte[] byteArray;
            try {
                byteArray = sb.toString().getBytes(s3Config.getEncoding());
            } catch (UnsupportedEncodingException e) {
                throw new ChunJunRuntimeException(e);
            }
            log.info("Upload part size：" + byteArray.length);

            if (s3Config.isEnableWriteSingleRecordAsFile()) {
                S3Util.putStringObject(
                        amazonS3, s3Config.getBucket(), s3Config.getObject(), sb.toString());
            } else {
                PartETag partETag =
                        S3Util.uploadPart(
                                amazonS3,
                                s3Config.getBucket(),
                                s3Config.getObject(),
                                this.currentUploadId,
                                this.currentPartNumber,
                                byteArray);

                MyPartETag myPartETag = new MyPartETag(partETag);
                myPartETags.add(myPartETag);
            }

            log.debug(
                    "task-{} upload etag:[{}]",
                    taskNumber,
                    myPartETags.stream().map(Objects::toString).collect(Collectors.joining(",")));
            writerUtil.close();
            writerUtil = null;
            sw = null;
        }
    }

    private void completeMultipartUploadFile() {
        if (s3Config.isEnableWriteSingleRecordAsFile()) {
            return;
        }
        if (this.currentPartNumber > 10000) {
            throw new IllegalArgumentException("part can not bigger than 10000");
        }
        List<PartETag> partETags =
                myPartETags.stream().map(MyPartETag::genPartETag).collect(Collectors.toList());
        if (partETags.size() > 0) {
            log.info(
                    "Start merging files partETags:{}",
                    partETags.stream().map(PartETag::getETag).collect(Collectors.joining(",")));
            S3Util.completeMultipartUpload(
                    amazonS3,
                    s3Config.getBucket(),
                    s3Config.getObject(),
                    this.currentUploadId,
                    partETags);
        } else {
            S3Util.abortMultipartUpload(
                    amazonS3, s3Config.getBucket(), s3Config.getObject(), this.currentUploadId);
            S3Util.putStringObject(amazonS3, s3Config.getBucket(), s3Config.getObject(), "");
        }
    }

    @Override
    public void closeInternal() {
        // Before closing the client, upload the remaining data smaller than 5M
        willClose = true;
        flushDataInternal();
        completeMultipartUploadFile();
        S3Util.closeS3(amazonS3);
        log.info("S3Client close!");
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
            stringRecord = (String[]) rowConverter.toExternal(rowData, stringRecord);
            try {
                int columnSize = columnNameList.size();
                if (s3Config.isEnableWriteSingleRecordAsFile()) {
                    columnSize = 1;
                }
                for (int i = 0; i < columnSize; ++i) {

                    String column = stringRecord[i];

                    if (column == null) {
                        continue;
                    }
                    writerUtil.write(column);
                }
                writerUtil.endRecord();

                if (s3Config.isEnableWriteSingleRecordAsFile()) {
                    Map<String, String> metadataMap =
                            GsonUtil.GSON.fromJson(stringRecord[1], Map.class);
                    String key = FilenameUtils.getPath(s3Config.getObject());
                    // 是否保留原始文件名
                    if (s3Config.isKeepOriginalFilename()) {
                        key += metadataMap.get(ORIGINAL_FILENAME) + getExtension();
                    } else {
                        key +=
                                jobId
                                        + "_"
                                        + taskNumber
                                        + "_"
                                        + UUID.randomUUID().toString()
                                        + getExtension();
                    }
                    s3Config.setObject(key);
                }
                flushDataInternal();
            } catch (Exception ex) {
                String msg = "RowData2string error RowData(" + rowData + ")";
                throw new WriteRecordException(msg, ex, 0, rowData);
            }
        } catch (Exception ex) {
            throw new WriteRecordException(ex.getMessage(), ex);
        }
    }

    public void setS3Conf(S3Config s3Config) {
        this.s3Config = s3Config;
    }
}

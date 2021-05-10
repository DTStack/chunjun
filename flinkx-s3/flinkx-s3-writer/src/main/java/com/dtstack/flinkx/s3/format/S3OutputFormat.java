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

package com.dtstack.flinkx.s3.format;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.BaseFileOutputFormat;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.s3.S3Config;
import com.dtstack.flinkx.s3.S3Util;
import com.dtstack.flinkx.s3.WriterUtil;
import com.dtstack.flinkx.util.StringUtil;
import com.dtstack.flinkx.util.SysUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The OutputFormat Implementation which write data to Amazon S3.
 * <p>
 * company www.dtstack.com
 *
 * @author jier
 */
public class S3OutputFormat extends BaseFileOutputFormat {

    private transient AmazonS3 amazonS3;
    private S3Config s3Config;

    protected List<String> columnNames;
    protected List<String> columnTypes;

    //必须从1开始，且大小不能大于10,000
    private transient int currentPartNumber;
    private transient String currentUploadId;
    private transient StringWriter sw;
    private transient List<MyPartETag> partETags;

    private transient ObjectMapper mapper;
    private static final String OVERWRITE_MODE = "overwrite";
    private transient WriterUtil writerUtil;

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        openSource();
        restore();
        actionBeforeWriteData();

        nextBlock();
    }

    private void restore() throws JsonProcessingException {
        if(formatState != null && formatState.getState() != null){
            Tuple2<String,List<MyPartETag>> state = (Tuple2<String,List<MyPartETag>>) formatState.getState();
            this.currentUploadId = state.f0;
            S3Util.putS3Object(amazonS3,s3Config.getBucket(), s3Config.getObject().get(0) + ".updateId", this.currentUploadId);
            List<MyPartETag> myPartETags = state.f1;
            partETags.addAll(myPartETags);
            String partETagJson = mapper.writeValueAsString(partETags);
            if(S3Util.doesObjectExist(amazonS3,s3Config.getBucket(),
                    s3Config.getObject().get(0) + ".etag" + SP + taskNumber)){
                S3Util.deleteObject(amazonS3,s3Config.getBucket(),
                        s3Config.getObject().get(0) + ".etag" + SP + taskNumber);
            }
            S3Util.putS3Object(amazonS3,s3Config.getBucket(), s3Config.getObject().get(0) + ".etag" + SP + taskNumber, partETagJson);

        }
    }

    @Override
    protected void cleanDirtyData() {

    }

    @Override
    protected void createActionFinishedTag() {
        if(currentUploadId == null || "".equals(currentUploadId.trim())){
            this.currentUploadId = S3Util.initiateMultipartUploadAndGetId(
                    amazonS3,s3Config.getBucket(), s3Config.getObject().get(0));
            S3Util.putS3Object(amazonS3,s3Config.getBucket(),
                    s3Config.getObject().get(0) + ".updateId", this.currentUploadId);
        }
    }

    /*
     * 第一个通道完成写数据前的操作
     */
    @Override
    protected void waitForActionFinishedBeforeWrite() {
        boolean readyWrite = S3Util.doesObjectExist(amazonS3,s3Config.getBucket(), s3Config.getObject().get(0) + ".updateId");

        int n = 0;
        while (!readyWrite) {
            if (n > SECOND_WAIT) {
                throw new RuntimeException("Wait action finished before write timeout");
            }

            SysUtil.sleep(1000);
            LOG.info("action finished tag path:{}", actionFinishedTag);
            readyWrite = S3Util.doesObjectExist(amazonS3,s3Config.getBucket(), s3Config.getObject().get(0) + ".updateId");
            n++;
        }
        this.currentUploadId = S3Util.getObjectContextAsString(amazonS3,s3Config.getBucket(), s3Config.getObject().get(0) + ".updateId");

    }

    @Override
    protected void flushDataInternal() throws IOException {
        LOG.debug("task-{} flush Data flushDataInternal,currentUploadId=[{}],currentPartNumber=[{}].", taskNumber, currentUploadId, currentPartNumber);
        byte[] byteArray = sw.getBuffer().toString().getBytes(s3Config.getEncoding());

        PartETag partETag = S3Util.uploadPart(amazonS3,s3Config.getBucket(),
                s3Config.getObject().get(0),
                this.currentUploadId,
                this.currentPartNumber,
                byteArray);

        MyPartETag myPartETag = new MyPartETag(partETag);
        partETags.add(myPartETag);
        String partETagJson = mapper.writeValueAsString(partETags);
        if(S3Util.doesObjectExist(amazonS3,s3Config.getBucket(), s3Config.getObject().get(0) + ".etag" + SP + taskNumber)){
            S3Util.deleteObject(amazonS3,s3Config.getBucket(), s3Config.getObject().get(0) + ".etag" + SP + taskNumber);
        }
        S3Util.putS3Object(amazonS3,s3Config.getBucket(), s3Config.getObject().get(0) + ".etag" + SP + taskNumber, partETagJson);

        LOG.debug("task-{} upload etag:[{}]", taskNumber, partETags.stream().map(Objects::toString).collect(Collectors.joining(",")));
        writerUtil.close();
        writerUtil = null;
    }

    @Override
    protected void writeSingleRecordToFile(Row row) throws WriteRecordException {
        try {
            if (this.writerUtil == null) {
                nextBlock();
            }
            // convert row to string
            int cnt = row.getArity();
            int i = 0;
            try {
                for (; i < cnt; ++i) {

                    Object column = row.getField(i);

                    if(column == null) {
                        continue;
                    }
                    writerUtil.write(StringUtil.col2string(column, columnTypes.get(i)));
                }
                writerUtil.endRecord();
            } catch(Exception ex) {
                String msg = "StringUtil.row2string error: when converting field[" + i + "] in Row(" + row + ")";
                throw new WriteRecordException(msg, ex, i, row);
            }

            rowsOfCurrentBlock++;
            if (restoreConfig.isRestore()) {
                lastRow = row;
            }
            TimeUnit.MILLISECONDS.sleep(50);
        } catch (Exception ex) {
            throw new WriteRecordException(ex.getMessage(), ex);
        }
    }

    @Override
    protected void nextBlock() {
        sw = new StringWriter();
        this.writerUtil = new WriterUtil(sw, s3Config.getFieldDelimiter());
        this.currentPartNumber = this.currentPartNumber + numTasks;
    }

    @Override
    protected void createFinishedTag() throws IOException {
        S3Util.putS3Object(amazonS3,s3Config.getBucket(),
                s3Config.getObject().get(0) + ".finished" + SP + taskNumber, "true");
        LOG.info("task is finished");
    }

    @Override
    protected void moveTemporaryDataBlockFileToDirectory() {

    }

    @Override
    protected void waitForAllTasksToFinish() throws IOException {
        final int maxRetryTime = 100;
        int i = 0;
        for (; i < maxRetryTime; i++) {
            List<String> finishedTags = S3Util.listObjectsKeyByPrefix(amazonS3, s3Config.getBucket(), s3Config.getObject().get(0) + ".finished" + SP);
            int finishedTaskNum = finishedTags.size();
            LOG.info("The number of finished task is:{}", finishedTaskNum);
            if (finishedTaskNum == numTasks) {
                break;
            }
            SysUtil.sleep(3000);
        }

        if (i == maxRetryTime) {
//            ftpHandler.deleteAllFilesInDir(finishedPath, null);
            throw new RuntimeException("timeout when gathering finish tags for each subtasks");
        }


    }

    @Override
    protected void coverageData() throws IOException {

    }

    @Override
    protected void moveTemporaryDataFileToDirectory() throws IOException {

    }

    @Override
    protected void moveAllTemporaryDataFileToDirectory() throws IOException {
        List<MyPartETag> myETagsResult = new ArrayList<>();
        List<S3ObjectSummary> etagsList = S3Util.listObjectsByPrefix(amazonS3, s3Config.getBucket(), s3Config.getObject().get(0) + ".etag" + SP);
        for (S3ObjectSummary etags : etagsList) {
            String etagsJson = S3Util.getObjectContextAsString(amazonS3,s3Config.getBucket(), etags.getKey());
            List<MyPartETag> eTagList = mapper.readValue(etagsJson, new TypeReference<List<MyPartETag>>() {
            });
            myETagsResult.addAll(eTagList);
        }
        List<PartETag> partETags = myETagsResult.stream().map(MyPartETag::genPartETag).collect(Collectors.toList());

        S3Util.completeMultipartUpload(amazonS3,s3Config.getBucket(), s3Config.getObject().get(0),this.currentUploadId, partETags);

        LOG.debug("task-{} all etag info:[{}]", taskNumber, myETagsResult.stream().map(Objects::toString).collect(Collectors.joining(",")));

    }

    @Override
    protected void checkOutputDir() {
        if (S3Util.doesObjectExist(amazonS3,s3Config.getBucket(), s3Config.getObject().get(0))) {
            if (OVERWRITE_MODE.equalsIgnoreCase(s3Config.getWriteMode()) && !SP.equals(outputFilePath)) {
                S3Util.deleteObject(amazonS3,s3Config.getBucket(), s3Config.getObject().get(0));
            }
        }
    }

    @Override
    protected void openSource() throws IOException {
        this.amazonS3 = S3Util.initS3(s3Config);
        this.partETags = new ArrayList<>();
        this.mapper = new ObjectMapper();
        this.currentPartNumber = taskNumber - numTasks + 1;
    }

    /**
     * 不能在该方法中关闭 amazonS3 连接，因为在{@link BaseFileOutputFormat#afterCloseInternal()}方法中会在此方法后面
     * 调用{@link S3OutputFormat#clearTemporaryDataFiles()} 去删除临时文件，而该方法需要使用 amazonS3 的连接
     */
    @Override
    protected void closeSource() throws IOException {
    }

    @Override
    protected void clearTemporaryDataFiles() throws IOException {
        //临时文件：
        //  1. object.updateId ，临时存放本次写入的 updateId
        //  2. object.etag/{$taskNumber} ，临时存放已经写入完成的 PartETag 对象
        //  3. object.finished/{$taskNumber} ，临时存放该通过写完的标记

        String object = s3Config.getObject().get(0);
        List<String> etags = S3Util.listObjectsKeyByPrefix(amazonS3, s3Config.getBucket(), object + ".etag" + SP);
        List<String> finisheds = S3Util.listObjectsKeyByPrefix(amazonS3, s3Config.getBucket(), object + ".finished" + SP);
        List<String> all = new ArrayList<>();
        all.addAll(etags);
        all.addAll(finisheds);

        // Verify that the objects were deleted successfully.
        DeleteObjectsResult delObjRes = S3Util.batchDelete(amazonS3,s3Config.getBucket(),all);
        int successfulDeletes = delObjRes.getDeletedObjects().size();
        LOG.info("{} objects successfully deleted.", successfulDeletes);
    }

    @Override
    protected void afterCloseInternal() {
        super.afterCloseInternal();
        S3Util.closeS3(amazonS3);
    }

    @Override
    public float getDeviation() {
        return 1.0F;
    }

    @Override
    protected String getExtension() {
        int index = s3Config.getObject().get(0).lastIndexOf(".");
        return index < 0 ? ".csv" : s3Config.getObject().get(0).substring(index);
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        notSupportBatchWrite("S3Writer");
    }

    public S3Config getS3Config() {
        return s3Config;
    }

    public void setS3Config(S3Config s3Config) {
        this.s3Config = s3Config;
    }

    @Override
    public FormatState getFormatState(){
        super.getFormatState();
        if (formatState != null) {
            formatState.setNumOfSubTask(taskNumber);
            formatState.setState(new Tuple2<>(currentUploadId, partETags));
        }
        return formatState;
    }
}

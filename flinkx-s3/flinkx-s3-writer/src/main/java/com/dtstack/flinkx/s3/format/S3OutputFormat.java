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
import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.BaseFileOutputFormat;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.s3.S3Config;
import com.dtstack.flinkx.s3.S3Util;
import com.dtstack.flinkx.s3.WriterUtil;
import com.dtstack.flinkx.util.StringUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.io.*;
import java.util.*;
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
    private transient List<MyPartETag> myPartETags;

    private boolean willClose = false;
    private static final String OVERWRITE_MODE = "overwrite";
    private transient WriterUtil writerUtil;

    private final static long MINSIZE = 1024 * 1024 * 5L;

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        openSource();
        restore();
        actionBeforeWriteData();
        nextBlock();
    }

    @Override
    protected boolean needWaitBeforeWriteRecords() {
        return true;
    }

    @Override
    protected void beforeWriteRecords() {
        if(s3Config.isFirstLineHeader()){
            Row firstRow = new Row(columnNames.size());
            for (int i = 0; i < columnNames.size(); i++) {
                firstRow.setField(i, columnNames.get(i));
            }
            try {
                writeFirstRecord(firstRow);
            } catch (WriteRecordException e) {
                e.printStackTrace();
                LOG.warn("first line fail to write");
            }
        }
    }

    private void restore() throws JsonProcessingException {
        if(formatState != null && formatState.getState() != null){
            Tuple2<String,List<MyPartETag>> state = (Tuple2<String,List<MyPartETag>>) formatState.getState();
            this.currentUploadId = state.f0;
            this.myPartETags = state.f1;
        }
    }

    @Override
    protected void cleanDirtyData() {

    }

    @Override
    protected void createActionFinishedTag() {
        if(currentUploadId == null || "".equals(currentUploadId.trim())){
            this.currentUploadId = S3Util.initiateMultipartUploadAndGetId(
                    amazonS3,s3Config.getBucket(), s3Config.getObject());
        }
    }

    /*
     * 第一个通道完成写数据前的操作
     */
    @Override
    protected void waitForActionFinishedBeforeWrite() {

    }


    @Override
    protected void flushDataInternal() throws IOException {
        LOG.debug("task-{} flush Data flushDataInternal,currentUploadId=[{}],currentPartNumber=[{}].", taskNumber, currentUploadId, currentPartNumber);
        StringBuffer sb = sw.getBuffer();
        if(sb.length() > MINSIZE || willClose){
            byte[] byteArray = sb.toString().getBytes(s3Config.getEncoding());
            PartETag partETag = S3Util.uploadPart(amazonS3,s3Config.getBucket(),
                    s3Config.getObject(),
                    this.currentUploadId,
                    this.currentPartNumber,
                    byteArray);

            MyPartETag myPartETag = new MyPartETag(partETag);
            myPartETags.add(myPartETag);

            LOG.debug("task-{} upload etag:[{}]", taskNumber, myPartETags.stream().map(Objects::toString).collect(Collectors.joining(",")));
            writerUtil.close();
            writerUtil = null;
        }else {
            LOG.debug("task-{} flush fail because of the data length is small than minPartSize({})", taskNumber, MINSIZE);
        }
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
        } catch (Exception ex) {
            throw new WriteRecordException(ex.getMessage(), ex);
        }
    }


    private void writeFirstRecord(Row row) throws WriteRecordException {
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
                    writerUtil.write(StringUtil.col2string(column, ColumnType.STRING.name()));
                }
                writerUtil.endRecord();
            } catch(Exception ex) {
                String msg = "StringUtil.row2string error: when converting field[" + i + "] in Row(" + row + ")";
                throw new WriteRecordException(msg, ex, i, row);
            }
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

    }

    @Override
    protected void moveTemporaryDataBlockFileToDirectory() {

    }

    @Override
    protected void waitForAllTasksToFinish() throws IOException {

    }

    @Override
    protected void coverageData() throws IOException {

    }

    @Override
    protected void moveTemporaryDataFileToDirectory() throws IOException {

    }

    @Override
    protected void moveAllTemporaryDataFileToDirectory() throws IOException {
        if(this.currentPartNumber > 10000){
            throw new IOException("part can not bigger than 10000");
        }
        List<PartETag> partETags = myPartETags.stream().map(MyPartETag::genPartETag).collect(Collectors.toList());
        if(partETags.size() > 0){
            // 说明上游有数据
            S3Util.completeMultipartUpload(amazonS3,s3Config.getBucket(), s3Config.getObject(),this.currentUploadId, partETags);
        }else{
            // 说明上游没有数据，取消文件上传，建立空文件
            S3Util.abortMultipartUpload(amazonS3,s3Config.getBucket(),s3Config.getObject(),this.currentUploadId);
            S3Util.putStringObject(amazonS3,s3Config.getBucket(),s3Config.getObject(),"");
        }
    }

    @Override
    protected void checkOutputDir() {
        if (S3Util.doesObjectExist(amazonS3,s3Config.getBucket(), s3Config.getObject())) {
            if (OVERWRITE_MODE.equalsIgnoreCase(s3Config.getWriteMode()) && !SP.equals(outputFilePath)) {
                S3Util.deleteObject(amazonS3,s3Config.getBucket(), s3Config.getObject());
            }
        }
    }

    @Override
    protected void openSource() throws IOException {
        this.amazonS3 = S3Util.initS3(s3Config);
        this.myPartETags = new ArrayList<>();
        this.currentPartNumber = taskNumber - numTasks + 1;
    }

    @Override
    public void closeInternal() throws IOException {
        readyCheckpoint = false;
        //最后触发一次 block文件重命名，为 .data 目录下的文件移动到数据目录做准备
        if(isTaskEndsNormally()){
            this.willClose = true;
            flushData();
            //restore == false 需要主动执行
            if (!restoreConfig.isRestore()) {
                moveTemporaryDataBlockFileToDirectory();
            }
        }
        numWriteCounter.add(sumRowsOfBlock);
    }

    @Override
    protected void closeSource() throws IOException {
    }

    @Override
    protected void clearTemporaryDataFiles() throws IOException {

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
        int index = s3Config.getObject().lastIndexOf(".");
        return index < 0 ? ".csv" : s3Config.getObject().substring(index);
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
            formatState.setState(new Tuple2<>(currentUploadId, myPartETags));
        }
        return formatState;
    }
}

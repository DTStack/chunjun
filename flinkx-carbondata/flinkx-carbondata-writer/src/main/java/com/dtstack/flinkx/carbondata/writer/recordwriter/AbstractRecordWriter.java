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


package com.dtstack.flinkx.carbondata.writer.recordwriter;


import com.dtstack.flinkx.carbondata.writer.dict.CarbonDictionaryUtil;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.writer.CarbonIndexFileMergeWriter;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.model.CarbonLoadModelBuilder;
import org.apache.carbondata.processing.loading.model.LoadOption;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.IOException;
import java.util.*;

/**
 * Abstract record writer wrapper
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public abstract class AbstractRecordWriter {

    protected CarbonTable carbonTable;

    protected List<CarbonLoadModel> carbonLoadModelList = new ArrayList<>();

    protected List<RecordWriter> recordWriterList = new ArrayList<>();

    protected List<TaskAttemptContext> taskAttemptContextList = new ArrayList<>();

    protected int[] counter = new int[1];

    protected List<String> fullColumnNames;

    protected List<DataType> fullColumnTypes;

    protected List<String[]> data = new ArrayList<>();

    public AbstractRecordWriter(CarbonTable carbonTable) {
        this.carbonTable = carbonTable;
    }

    /**
     * get record index
     * @param record record
     * @return index
     */
    protected abstract int getRecordWriterNumber(String[] record);

    protected TaskAttemptContext createTaskContext() {
        Random random = new Random();
        JobID jobId = new JobID(UUID.randomUUID().toString(), 0);
        TaskID task = new TaskID(jobId, TaskType.MAP, random.nextInt());
        TaskAttemptID attemptId = new TaskAttemptID(task, random.nextInt());
        Configuration conf = new Configuration(FileFactory.getConfiguration());
        return new TaskAttemptContextImpl(conf, attemptId);
    }

    public void write(String[] record) throws IOException, InterruptedException {
        data.add(record);
    }

    protected void closeRecordWriter(int writerNo) throws IOException, InterruptedException {
        RecordWriter recordWriter = recordWriterList.get(writerNo);
        if(recordWriter != null) {
            recordWriter.close(taskAttemptContextList.get(writerNo));
            if(counter[writerNo] != 0) {
                postCloseRecordWriter(writerNo);
            }
        }
    }

    protected void postCloseRecordWriter(int writerNo) throws IOException {
        final CarbonLoadModel carbonLoadModel = carbonLoadModelList.get(writerNo);
        String segmentFileName = SegmentFileStore.writeSegmentFile(carbonTable, carbonLoadModel.getSegmentId(),
                String.valueOf(carbonLoadModel.getFactTimeStamp()));

        SegmentFileStore.updateSegmentFile(
                carbonTable,
                carbonLoadModel.getSegmentId(),
                segmentFileName,
                carbonTable.getCarbonTableIdentifier().getTableId(),
                new SegmentFileStore(carbonTable.getTablePath(), segmentFileName));


        LoadMetadataDetails metadataDetails = carbonLoadModel.getCurrentLoadMetadataDetail();
        metadataDetails.setSegmentFile(segmentFileName);

        CarbonLoaderUtil.populateNewLoadMetaEntry(
                metadataDetails,
                SegmentStatus.SUCCESS,
                carbonLoadModel.getFactTimeStamp(),
                true);

        CarbonLoaderUtil.addDataIndexSizeIntoMetaEntry(metadataDetails, carbonLoadModel.getSegmentId(), carbonTable);

        boolean done = CarbonLoaderUtil.recordNewLoadMetadata(metadataDetails, carbonLoadModel, false, false, "");

        if(!done) {
            throw new RuntimeException("Failed to recordNewLoadMetadata");
        }

        new CarbonIndexFileMergeWriter(carbonTable)
                .mergeCarbonIndexFilesOfSegment(carbonLoadModel.getSegmentId(),
                        carbonLoadModel.getTablePath(),
                        false,
                        String.valueOf(carbonLoadModel.getFactTimeStamp())
                );

    }

    public void close() throws IOException, InterruptedException {
        if(data.isEmpty()) {
            return;
        }

        CarbonDictionaryUtil.generateGlobalDictionary(carbonLoadModelList.get(0), data);

        createRecordWriterList();

        for(String[] record : data) {
            int writerNo = getRecordWriterNumber(record);
            ObjectArrayWritable writable = new ObjectArrayWritable();
            writable.set(record);
            recordWriterList.get(writerNo).write(NullWritable.get(), writable);
            counter[writerNo]++;
        }

        data.clear();

        for(int i = 0; i < recordWriterList.size(); ++i) {
            closeRecordWriter(i);
        }
    }

    /**
     * add recordWriter to recordWriter list
     */
    protected abstract void createRecordWriterList();

    protected RecordWriter createRecordWriter(CarbonLoadModel model, TaskAttemptContext context) throws IOException {

        CarbonTableOutputFormat.setLoadModel(context.getConfiguration(), model);
        CarbonTableOutputFormat.setCarbonTable(context.getConfiguration(), model.getCarbonDataLoadSchema().getCarbonTable());
        CarbonTableOutputFormat carbonTableOutputFormat = new CarbonTableOutputFormat();

        return carbonTableOutputFormat.getRecordWriter(context);
    }

    protected CarbonLoadModel createCarbonLoadModel() {
        CarbonLoadModel carbonLoadModel = new CarbonLoadModel();
        carbonLoadModel.setParentTablePath(null);
        carbonLoadModel.setFactFilePath("");
        carbonLoadModel.setCarbonTransactionalTable(carbonTable.getTableInfo().isTransactionalTable());
        carbonLoadModel.setAggLoadRequest(false);
        carbonLoadModel.setSegmentId("");

        fullColumnNames = new ArrayList<>();
        fullColumnTypes = new ArrayList<>();

        List<ColumnSchema> columnSchemas = carbonTable.getTableInfo().getFactTable().getListOfColumns();
        for(int i = 0; i < columnSchemas.size(); ++i) {
            ColumnSchema columnSchema = columnSchemas.get(i);
            if(!columnSchema.isInvisible()) {
                fullColumnNames.add(columnSchema.getColumnName());
                fullColumnTypes.add(columnSchema.getDataType());
            }
        }

        Map<String,String> options = Collections.singletonMap("fileheader", StringUtils.join(fullColumnNames, ","));

        Map<String,String> optionsFinal = null;
        try {
            Map<String,String> tableProperties = carbonTable.getTableInfo().getFactTable().getTableProperties();
            optionsFinal = LoadOption.fillOptionWithDefaultValue(options);
            optionsFinal.put("sort_scope", tableProperties.getOrDefault("sort_scope", CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT));
            new CarbonLoadModelBuilder(carbonTable).build(
                    options,
                    optionsFinal,
                    carbonLoadModel,
                    FileFactory.getConfiguration(),
                    new HashMap<>(0),
                    true
            );
            CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(carbonLoadModel, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return carbonLoadModel;
    }
}

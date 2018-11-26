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
package com.dtstack.flinkx.carbondata.writer;

import com.dtstack.flinkx.carbondata.CarbondataUtil;
import com.dtstack.flinkx.common.ColumnType;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import com.dtstack.flinkx.util.DateUtil;
import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.CarbonIndexFileMergeWriter;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable;
import org.apache.carbondata.processing.loading.TableProcessingOperations;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.model.CarbonLoadModelBuilder;
import org.apache.carbondata.processing.loading.model.LoadOption;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.types.Row;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Carbondata Output Format
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class CarbonOutputFormat extends RichOutputFormat implements CleanupWhenUnsuccessful {

    protected Map<String,String> hadoopConfig;

    protected String table;

    protected String database;

    protected String path;

    protected List<String> column;

    private CarbonTable carbonTable;

    private Map<String,String> options = new HashMap<>();

    private CarbonLoadModel carbonLoadModel = new CarbonLoadModel();

    private TaskAttemptContext taskAttemptContext;

    private RecordWriter recordWriter;

    private List<String> fullColumnNames;

    private List<String> fullColumnTypes;

    private List<Integer> fullColumnIndices;


    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        CarbondataUtil.initFileFactory(hadoopConfig);
        carbonTable = CarbondataUtil.buildCarbonTable(database, table, path);
        System.out.println(carbonTable);
        CarbonProperties carbonProperty = CarbonProperties.getInstance();
        carbonProperty.addProperty("zookeeper.enable.lock", "false");
        //buildCarbonLoadModel();
        Map<String,String> tableProperties = carbonTable.getTableInfo().getFactTable().getTableProperties();
        carbonLoadModel.setParentTablePath(null);
        carbonLoadModel.setFactFilePath("");
        carbonLoadModel.setCarbonTransactionalTable(carbonTable.getTableInfo().isTransactionalTable());
        carbonLoadModel.setAggLoadRequest(false);
        carbonLoadModel.setSegmentId("");

        inferFullColumnInfo();
        options.put("fileheader", StringUtils.join(fullColumnNames, ","));

        Map<String,String> optionsFinal = null;
        try {
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
        } catch (InvalidLoadOptionException e) {
            throw new RuntimeException(e);
        }

        TableProcessingOperations.deletePartialLoadDataIfExist(carbonTable, false);
        SegmentStatusManager.deleteLoadsAndUpdateMetadata(carbonTable, false, null);
        boolean isOverwriteTable = false;
        carbonLoadModel.setSegmentId(UUID.randomUUID().toString());

        List<CarbonDimension> allDimensions = carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().getAllDimensions();

        boolean createDictionary = false;
        if (!createDictionary) {
            carbonLoadModel.setUseOnePass(false);
        }
        CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(carbonLoadModel, isOverwriteTable);

        createRecordWriter();

    }

    private void inferFullColumnInfo() {
        fullColumnIndices = new ArrayList<>();
        fullColumnNames = new ArrayList<>();
        fullColumnTypes = new ArrayList<>();

        List<ColumnSchema> columnSchemas = carbonTable.getTableInfo().getFactTable().getListOfColumns();
        for(int i = 0; i < columnSchemas.size(); ++i) {
            ColumnSchema columnSchema = columnSchemas.get(i);
            fullColumnNames.add(columnSchema.getColumnName());
            fullColumnTypes.add(columnSchema.getDataType().getName());
        }

        for(int i = 0; i < fullColumnNames.size(); ++i) {
            fullColumnIndices.add(column.indexOf(fullColumnNames.get(i)));
        }
    }

    private void createRecordWriter() {
        try {
            System.out.println(carbonLoadModel.getSegmentId());
            CarbonTableOutputFormat.setLoadModel(FileFactory.getConfiguration(), carbonLoadModel);
            CarbonTableOutputFormat.setCarbonTable(FileFactory.getConfiguration(), carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable());
            CarbonTableOutputFormat carbonTableOutputFormat = new CarbonTableOutputFormat();
            taskAttemptContext = createTaskContext();
            recordWriter = carbonTableOutputFormat.getRecordWriter(taskAttemptContext);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    private TaskAttemptContext createTaskContext() {
        Random random = new Random();
        JobID jobId = new JobID(UUID.randomUUID().toString(), 0);
        TaskID task = new TaskID(jobId, TaskType.MAP, random.nextInt());
        TaskAttemptID attemptID = new TaskAttemptID(task, random.nextInt());
        TaskAttemptContextImpl context = new TaskAttemptContextImpl(FileFactory.getConfiguration(), attemptID);
        return context;
    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        int i = 0;
        try {
            ObjectArrayWritable writable = new ObjectArrayWritable();
            Object[] record = new Object[fullColumnNames.size()];
            for(; i < fullColumnIndices.size(); ++i) {
                int index = fullColumnIndices.get(i);
                if(index == -1) {
                    record[i] = null;
                } else {
                    ColumnType columnType = ColumnType.fromString(fullColumnTypes.get(i));
                    Object column = row.getField(index);
                    String rowData = column.toString();
                    Object val = null;
                    if(rowData.length() == 0 && columnType != ColumnType.STRING) {
                        record[i] = null;
                    } else {
                        switch (columnType) {
                            case SMALLINT:
                                val = Short.valueOf(rowData);
                                break;
                            case INT:
                                val = Integer.valueOf(rowData);
                                break;
                            case BIGINT:
                                BigInteger data = new BigInteger(rowData);
                                if (data.compareTo(new BigInteger(String.valueOf(Long.MAX_VALUE))) > 0) {
                                    val = data;
                                } else {
                                    val = Long.valueOf(rowData);
                                }
                                break;
                            case FLOAT:
                                val = Float.valueOf(rowData);
                                break;
                            case DOUBLE:
                                val = Double.valueOf(rowData);
                                break;
                            case DECIMAL:
                                val = new BigDecimal(rowData);
                                break;
                            case STRING:
                            case VARCHAR:
                            case CHAR:
                                val = rowData;
                                break;
                            case BOOLEAN:
                                val = Boolean.valueOf(rowData);
                                break;
                            case DATE:
                                val = DateUtil.columnToDate(column);
                                break;
                            case TIMESTAMP:
                                val = DateUtil.columnToTimestamp(column);
                                break;
                            default:
                                throw new IllegalArgumentException();
                        }
                    }
                    record[i] = val;
                }
            }
            writable.set(record);
            recordWriter.write(NullWritable.get(), writable);
        } catch(Exception e) {
            if(i < row.getArity()) {
                throw new WriteRecordException(recordConvertDetailErrorMessage(i, row), e, i, row);
            }
            throw new WriteRecordException(e.getMessage(), e);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        // CAN NOT HAPPEN
    }

    @Override
    public void tryCleanupOnError() throws Exception {
        // hehe
    }

    @Override
    public void closeInternal() throws IOException {
        if(recordWriter != null) {
            try {
                recordWriter.close(taskAttemptContext);
                updateTableStatusAfterDataLoad();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void updateTableStatusAfterDataLoad() throws IOException {
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


        boolean done = CarbonLoaderUtil.recordNewLoadMetadata(metadataDetails, carbonLoadModel, false,
                false, "");

        System.out.println(done);

        new CarbonIndexFileMergeWriter(carbonTable)
                .mergeCarbonIndexFilesOfSegment(carbonLoadModel.getSegmentId(),
                        carbonLoadModel.getTablePath(),
                        false,
                        segmentFileName.split("_")[1]);

    }
}

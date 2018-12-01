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
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Carbondata Output Format
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class CarbonOutputFormat extends RichOutputFormat implements CleanupWhenUnsuccessful {

    protected Map<String,String> hadoopConfig;

    protected String defaultFS;

    protected String table;

    protected String database;

    protected String path;

    protected List<String> column;

    protected boolean overwrite = false;

    private CarbonTable carbonTable;

    private Map<String,String> options = new HashMap<>();

    private CarbonLoadModel carbonLoadModel = new CarbonLoadModel();

    private TaskAttemptContext taskAttemptContext;

    private RecordWriter recordWriter;

    private List<String> fullColumnNames;

    private List<String> fullColumnTypes;

    private List<Integer> fullColumnIndices;

    private final int CARBON_BATCH_SIZE =  1024 * 100;

    private int insertedRecords = 0;

    private String bakPath;

    private FileSystem fs;

    @Override
    public void configure(Configuration parameters) {
        try {
            CarbondataUtil.initFileFactory(hadoopConfig, defaultFS);
            fs = FileSystem.get(FileFactory.getConfiguration());
            bakPath = path + "_bak";
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        if(overwrite) {
            carbonTable = CarbondataUtil.buildCarbonTable(database, table, bakPath);
        } else {
            carbonTable = CarbondataUtil.buildCarbonTable(database, table, path);
        }
        CarbonProperties carbonProperty = CarbonProperties.getInstance();
        carbonProperty.addProperty("zookeeper.enable.lock", "false");
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
        boolean isOverwriteTable = true;
        carbonLoadModel.setSegmentId(UUID.randomUUID().toString());

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

        column = column.stream().map(String::toLowerCase).collect(Collectors.toList());

        List<ColumnSchema> columnSchemas = carbonTable.getTableInfo().getFactTable().getListOfColumns();
        for(int i = 0; i < columnSchemas.size(); ++i) {
            ColumnSchema columnSchema = columnSchemas.get(i);
            if(!columnSchema.isInvisible()) {
                fullColumnNames.add(columnSchema.getColumnName());
                fullColumnTypes.add(columnSchema.getDataType().getName());
            }
        }

        for(int i = 0; i < fullColumnNames.size(); ++i) {
            fullColumnIndices.add(column.indexOf(fullColumnNames.get(i)));
        }
    }

    private void createRecordWriter() {
        try {
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
                    Object column = row.getField(index);
                    if(column == null) {
                        record[i] = null;
                    } else {
                        record[i] = StringUtil.col2string(column, fullColumnTypes.get(i));
                    }
                }
            }
            writable.set(record);
            recordWriter.write(NullWritable.get(), writable);
            insertedRecords++;
        } catch(Exception e) {
            if(i < row.getArity()) {
                throw new WriteRecordException(recordConvertDetailErrorMessage(i, row), e, i, row);
            }
            throw new WriteRecordException(e.getMessage(), e);
        }
        if(insertedRecords == CARBON_BATCH_SIZE) {
            closeRecordWriter();
            createRecordWriter();
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        // CAN NOT HAPPEN
        throw new IllegalArgumentException("It can not happen.");
    }

    @Override
    public void tryCleanupOnError() throws Exception {
        fs.delete(new Path(bakPath), true);
    }

    @Override
    protected boolean needWaitBeforeOpenInternal() {
        return overwrite;
    }

    @Override
    protected void beforeOpenInternal() {
        if(taskNumber == 0) {
            String schemaPath =  path + "/Metadata/schema";
            InputStream in = null;
            OutputStream out = null;
            try {
                if(fs.exists(new Path(bakPath))) {
                    fs.delete(new Path(bakPath), true);
                }
                fs.mkdirs(new Path(bakPath));
                out = fs.create(new Path(bakPath + "/Metadata/schema"));
                in = fs.open(new Path(schemaPath));
                IOUtils.copyBytes(in, out, 1024);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                if(in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                if(out != null) {
                    try {
                        out.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    @Override
    public void closeInternal() throws IOException {
        closeRecordWriter();
    }

    private void closeRecordWriter()  {
        if(recordWriter != null) {
            try {
                recordWriter.close(taskAttemptContext);
                updateTableStatusAfterDataLoad();
                insertedRecords = 0;
            } catch (Exception e) {
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

        if(!done) {
            throw new RuntimeException("Failed to recordNewLoadMetadata");
        }

        new CarbonIndexFileMergeWriter(carbonTable)
                .mergeCarbonIndexFilesOfSegment(carbonLoadModel.getSegmentId(),
                        carbonLoadModel.getTablePath(),
                        false,
                        segmentFileName.split("_")[1].split("\\.")[0]);

    }

    @Override
    protected boolean needWaitAfterCloseInternal() {
        return overwrite;
    }

    @Override
    protected void afterCloseInternal()  {
        if(taskNumber == 0 && overwrite) {
            try {
                fs.delete(new Path(path), true);
                fs.rename(new Path(bakPath), new Path(path));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}

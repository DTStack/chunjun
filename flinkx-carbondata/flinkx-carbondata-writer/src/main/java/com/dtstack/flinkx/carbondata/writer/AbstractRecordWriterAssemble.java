package com.dtstack.flinkx.carbondata.writer;


import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.SegmentFileStore;
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
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;



public abstract class AbstractRecordWriterAssemble {

    private static final int batchSize = 1024;

    protected CarbonTable carbonTable;

    protected List<CarbonLoadModel> carbonLoadModelList = new ArrayList<>();

    protected List<RecordWriter> recordwriterList = new ArrayList<>();

    protected List<TaskAttemptContext> taskAttemptContextList = new ArrayList<>();

    protected List<Integer> counterList = new ArrayList<>();


    public AbstractRecordWriterAssemble(CarbonTable carbonTable) {
        this.carbonTable = carbonTable;
    }

    protected abstract int getRecordWriterNumber(Object[] record);

    protected TaskAttemptContext createTaskContext() {
        Random random = new Random();
        JobID jobId = new JobID(UUID.randomUUID().toString(), 0);
        TaskID task = new TaskID(jobId, TaskType.MAP, random.nextInt());
        TaskAttemptID attemptID = new TaskAttemptID(task, random.nextInt());
        Configuration conf = new Configuration(FileFactory.getConfiguration());
        TaskAttemptContextImpl context = new TaskAttemptContextImpl(conf, attemptID);
        return context;
    }

    private void incCounter(int writerNo) {
        counterList.set(writerNo, getCounter(writerNo) + 1);
    }

    private int getCounter(int writerNo) {
        return counterList.get(writerNo);
    }

    private void clearCounter(int writerNo) {
        counterList.set(writerNo, 0);
    }

    protected void write(Object[] record) throws IOException, InterruptedException {
        int writerNo = getRecordWriterNumber(record);
        ObjectArrayWritable writable = new ObjectArrayWritable();
        writable.set(record);
        recordwriterList.get(writerNo).write(NullWritable.get(), writable);
        incCounter(writerNo);
        if(getCounter(writerNo) == batchSize) {
            closeRecordWriter(writerNo);
            recordwriterList.set(writerNo, createRecordWriter(carbonLoadModelList.get(writerNo) ,taskAttemptContextList.get(writerNo)));
            clearCounter(writerNo);
        }
    }

    protected void closeRecordWriter(int writerNo) throws IOException, InterruptedException {
        RecordWriter recordWriter = recordwriterList.get(writerNo);
        if(recordWriter != null) {
            recordWriter.close(taskAttemptContextList.get(writerNo));
            postCloseRecordWriter(writerNo);
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

    public void close() throws IOException, InterruptedException {
        for(int i = 0; i < recordwriterList.size(); ++i) {
            closeRecordWriter(i);
        }
    }

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

        List<String> fullColumnNames = new ArrayList<>();

        List<ColumnSchema> columnSchemas = carbonTable.getTableInfo().getFactTable().getListOfColumns();
        for(int i = 0; i < columnSchemas.size(); ++i) {
            ColumnSchema columnSchema = columnSchemas.get(i);
            if(!columnSchema.isInvisible()) {
                fullColumnNames.add(columnSchema.getColumnName());
            }
        }

        Map<String,String> options = new HashMap<>();
        options.put("fileheader", StringUtils.join(fullColumnNames, ","));

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

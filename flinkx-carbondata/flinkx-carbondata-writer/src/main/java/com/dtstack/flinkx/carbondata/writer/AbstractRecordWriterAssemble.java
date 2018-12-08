package com.dtstack.flinkx.carbondata.writer;

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

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
        TaskAttemptContextImpl context = new TaskAttemptContextImpl(FileFactory.getConfiguration(), attemptID);
        return context;
    }

    private void incCounter(int writerNo) {
        counterList.set(writerNo, getCounter(writerNo) + 1);
    }

    private int getCounter(int writerNo) {
        return counterList.get(writerNo);
    }

    protected void write(Object[] record) throws IOException, InterruptedException {
        int writerNo = getRecordWriterNumber(record);
        ObjectArrayWritable writable = new ObjectArrayWritable();
        writable.set(record);
        recordwriterList.get(writerNo).write(NullWritable.get(), writable);
        incCounter(writerNo);
        if(getCounter(writerNo) == batchSize) {
            closeRecordWriter(writerNo);
            //recordwriterList.set(writerNo, createRecordWriter());
        }
    }

    protected void closeRecordWriter(int writerNo) throws IOException, InterruptedException {
        RecordWriter recordWriter = recordwriterList.get(writerNo);
        if(recordWriter != null) {
            recordWriter.close(taskAttemptContextList.get(writerNo));
            postCloseRecordWriter();
        }
    }

    protected abstract void postCloseRecordWriter();

    public void close() throws IOException, InterruptedException {
        for(int i = 0; i < recordwriterList.size(); ++i) {
            closeRecordWriter(i);
        }
    }

    protected RecordWriter createRecordWriter(CarbonLoadModel model, TaskAttemptContext context) throws IOException {
        CarbonTableOutputFormat.setLoadModel(FileFactory.getConfiguration(), model);
        CarbonTableOutputFormat.setCarbonTable(FileFactory.getConfiguration(), model.getCarbonDataLoadSchema().getCarbonTable());
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
        return carbonLoadModel;
    }
}

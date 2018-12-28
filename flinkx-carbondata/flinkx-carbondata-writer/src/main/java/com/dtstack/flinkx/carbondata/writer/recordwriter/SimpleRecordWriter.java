package com.dtstack.flinkx.carbondata.writer.recordwriter;


import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;


public class SimpleRecordWriter extends AbstractRecordWriter {

    public SimpleRecordWriter(CarbonTable carbonTable) {
        super(carbonTable);
        CarbonLoadModel carbonLoadModel = createCarbonLoadModel();
        carbonLoadModelList.add(carbonLoadModel);
        TaskAttemptContext context = createTaskContext();
        taskAttemptContextList.add(context);

    }

    @Override
    protected int getRecordWriterNumber(String[] record) {
        return 0;
    }

    @Override
    protected void createRecordWriterList() {
        RecordWriter recordWriter = null;
        try {
            recordWriter = createRecordWriter(carbonLoadModelList.get(0), taskAttemptContextList.get(0));
            recordWriterList.add(recordWriter);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}

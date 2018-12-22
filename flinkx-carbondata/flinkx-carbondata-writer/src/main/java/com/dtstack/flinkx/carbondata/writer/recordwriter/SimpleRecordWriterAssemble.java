package com.dtstack.flinkx.carbondata.writer.recordwriter;


import com.dtstack.flinkx.carbondata.writer.recordwriter.AbstractRecordWriterAssemble;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;


public class SimpleRecordWriterAssemble extends AbstractRecordWriterAssemble {

    public SimpleRecordWriterAssemble(CarbonTable carbonTable) {
        super(carbonTable);
        CarbonLoadModel carbonLoadModel = createCarbonLoadModel();
        carbonLoadModelList.add(carbonLoadModel);
        TaskAttemptContext context = createTaskContext();
        taskAttemptContextList.add(context);
        RecordWriter recordWriter = null;
        try {
            recordWriter = createRecordWriter(carbonLoadModel, context);
            recordWriterList.add(recordWriter);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected int getRecordWriterNumber(Object[] record) {
        return 0;
    }

}

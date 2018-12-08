package com.dtstack.flinkx.carbondata.writer;


import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.partition.PartitionType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;


public class CarbonPartitionRecordWriterAssemble extends AbstractRecordWriterAssemble {

    private int partitionColNumber;

    private PartitionType partitionType;

    public CarbonPartitionRecordWriterAssemble(CarbonTable carbonTable) {
        super(carbonTable);
        PartitionInfo partitionInfo = carbonTable.getPartitionInfo();
        List<Integer> partitionIds =  partitionInfo.getPartitionIds();
        for(Integer partitionId : partitionIds) {
            CarbonLoadModel carbonLoadModel = createCarbonLoadModel();
            carbonLoadModelList.add(carbonLoadModel);
            TaskAttemptContext context = createTaskContext();
            context.getConfiguration().set("carbon.outputformat.taskno", String.valueOf(partitionId));
            taskAttemptContextList.add(context);
            counterList.add(new Integer(0));
            RecordWriter recordWriter = null;
            try {
                recordWriter = createRecordWriter(carbonLoadModel, context);
                recordwriterList.add(recordWriter);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        partitionType = partitionInfo.getPartitionType();


    }

    @Override
    protected int getRecordWriterNumber(Object[] record) {

        return 0;
    }


    @Override
    protected void postCloseRecordWriter() {

    }


}

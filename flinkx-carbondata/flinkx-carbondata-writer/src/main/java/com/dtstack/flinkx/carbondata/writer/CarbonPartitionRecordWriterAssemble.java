package com.dtstack.flinkx.carbondata.writer;


import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.partition.PartitionType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.partition.HashPartitioner;
import org.apache.carbondata.core.scan.partition.ListPartitioner;
import org.apache.carbondata.core.scan.partition.Partitioner;
import org.apache.carbondata.core.scan.partition.RangePartitioner;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;


public class CarbonPartitionRecordWriterAssemble extends AbstractRecordWriterAssemble {

    private int partitionColNumber;

    private Partitioner partitioner;

    private List<Integer> partitionIds;

    public CarbonPartitionRecordWriterAssemble(CarbonTable carbonTable) {
        super(carbonTable);
        PartitionInfo partitionInfo = carbonTable.getPartitionInfo();
        partitionIds =  partitionInfo.getPartitionIds();
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

        List<ColumnSchema> columnSchemaList =  partitionInfo.getColumnSchemaList();
        if(columnSchemaList.isEmpty()) {
            throw new IllegalArgumentException("no partition column");
        }
        if(columnSchemaList.size() > 1) {
            throw new UnsupportedOperationException("does not support more than one partition column");
        }

        partitionColNumber = -1;
        List<ColumnSchema> columnSchemas = carbonTable.getTableInfo().getFactTable().getListOfColumns();
        for(int i = 0; i < columnSchemas.size(); ++i) {
            ColumnSchema columnSchema = columnSchemas.get(i);
            if(!columnSchema.isInvisible() && columnSchema.getColumnName().equalsIgnoreCase(columnSchemaList.get(0).getColumnName())) {
                partitionColNumber = i;
                break;
            }
        }

        if(partitionColNumber == -1) {
            throw new RuntimeException("partition column not found in table schema");
        }

        PartitionType partitionType = partitionInfo.getPartitionType();
        int partitionNum = partitionInfo.getNumPartitions();
        if(partitionType == PartitionType.HASH) {
            partitioner = new HashPartitioner(partitionNum);
        } else if(partitionType == PartitionType.LIST) {
            partitioner = new ListPartitioner(partitionInfo);
        } else if(partitionType == PartitionType.RANGE) {
            partitioner = new RangePartitioner(partitionInfo);
        } else {
            throw new IllegalArgumentException("Unsupported Partitioner");
        }
    }

    @Override
    protected int getRecordWriterNumber(Object[] record) {
        Object v = record[partitionColNumber];
        int partitionId = partitioner.getPartition(v);
        return partitionIds.indexOf(partitionId);
    }

}

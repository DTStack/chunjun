package com.dtstack.flinkx.carbondata.writer.recordwriter;


import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;


public class CarbonPartitionRecordWriterAssemble extends AbstractRecordWriterAssemble {

    private int partitionColNumber;

    private Partitioner partitioner;

    private List<Integer> partitionIds;

    private PartitionType partitionType;

    private static ThreadLocal<SimpleDateFormat> dateFormat = ThreadLocal.withInitial(() -> {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        return sdf;
    });

    public CarbonPartitionRecordWriterAssemble(CarbonTable carbonTable) {
        super(carbonTable);
        PartitionInfo partitionInfo = carbonTable.getPartitionInfo();
        partitionIds =  partitionInfo.getPartitionIds();
        counter = new int[partitionIds.size()];
        for(Integer partitionId : partitionIds) {
            CarbonLoadModel carbonLoadModel = createCarbonLoadModel();
            carbonLoadModelList.add(carbonLoadModel);
            TaskAttemptContext context = createTaskContext();
            context.getConfiguration().set("carbon.outputformat.taskno", String.valueOf(partitionId));
            taskAttemptContextList.add(context);
            RecordWriter recordWriter = null;
            try {
                recordWriter = createRecordWriter(carbonLoadModel, context);
                recordWriterList.add(recordWriter);
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

        partitionType = partitionInfo.getPartitionType();
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
        DataType dataType = fullColumnTypes.get(partitionColNumber);
        if(partitionType == PartitionType.RANGE) {
            SimpleDateFormat format = null;
            if(dataType == DataTypes.DATE) {
                format = dateFormat.get();
            } else if(dataType == DataTypes.TIMESTAMP) {
                format = DateUtil.getDateTimeFormatter();
            }
            v = StringUtil.string2col((String)v, dataType.getName(), format);
            if(v instanceof Date) {
                Date date = (Date) v;
                v = date.getTime();
            }
        }
        int partitionId = partitioner.getPartition(v);
        return partitionIds.indexOf(partitionId);
    }

}

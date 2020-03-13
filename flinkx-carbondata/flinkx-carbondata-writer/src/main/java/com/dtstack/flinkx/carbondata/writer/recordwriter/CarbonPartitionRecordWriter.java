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


import com.dtstack.flinkx.carbondata.writer.dict.CarbonTypeConverter;
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
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;


/**
 * record writer for carbon parititional table
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class CarbonPartitionRecordWriter extends AbstractRecordWriter {

    private int partitionColNumber;

    private Partitioner partitioner;

    private List<Integer> partitionIds;

    private PartitionType partitionType;

    private static final String NULL_FORMAT = "\\N";

    private SimpleDateFormat dateFormat;

    private SimpleDateFormat timestampFormat;

    private void initDateFormat() {
        dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        timestampFormat.setTimeZone(TimeZone.getTimeZone("GMT+8"));
    }

    public CarbonPartitionRecordWriter(CarbonTable carbonTable) {
        super(carbonTable);

        initDateFormat();

        PartitionInfo partitionInfo = carbonTable.getPartitionInfo();
        partitionIds =  partitionInfo.getPartitionIds();
        counter = new int[partitionIds.size()];
        for(Integer partitionId : partitionIds) {
            CarbonLoadModel carbonLoadModel = createCarbonLoadModel();
            carbonLoadModelList.add(carbonLoadModel);
            TaskAttemptContext context = createTaskContext();
            context.getConfiguration().set("carbon.outputformat.taskno", String.valueOf(partitionId));
            taskAttemptContextList.add(context);
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
    protected int getRecordWriterNumber(String[] record) {
        Object v = record[partitionColNumber];
        DataType dataType = fullColumnTypes.get(partitionColNumber);
        if(v != null && !v.equals(NULL_FORMAT)) {
            try {
                v = CarbonTypeConverter.string2col((String)v, dataType, NULL_FORMAT, timestampFormat, dateFormat);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            if(v instanceof Date) {
                Date date = (Date) v;
                v = date.getTime();
            } else if(v instanceof String && partitioner instanceof RangePartitioner) {
                String s = (String) v;
                v = s.getBytes(StandardCharsets.UTF_8);
            }
        } else {
            v = null;
        }
        int partitionId = partitioner.getPartition(v);
        return partitionIds.indexOf(partitionId);
    }

    @Override
    protected void createRecordWriterList() {
        for(int i = 0; i < partitionIds.size(); ++i) {
            RecordWriter recordWriter = null;
            try {
                recordWriter = createRecordWriter(carbonLoadModelList.get(i), taskAttemptContextList.get(i));
                recordWriterList.add(recordWriter);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}

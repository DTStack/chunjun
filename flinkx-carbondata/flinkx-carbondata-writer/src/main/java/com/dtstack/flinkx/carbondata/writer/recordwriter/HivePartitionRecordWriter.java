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


import com.dtstack.flinkx.carbondata.writer.TaskNumberGenerator;
import com.dtstack.flinkx.carbondata.writer.dict.CarbonTypeConverter;
import com.dtstack.flinkx.constants.ConstantValue;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * record writer for hive partition table
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class HivePartitionRecordWriter extends AbstractRecordWriter {

    private String partition;

    private String taskNumber;

    private String writePath;

    private CarbonLoadModel carbonLoadModel;

    private TaskAttemptContext context;


    public HivePartitionRecordWriter(CarbonTable carbonTable, String partition) {
        super(carbonTable);
        this.partition = updatePartition(carbonTable, partition);
        writePath = carbonTable.getTablePath() + "/" + this.partition;
        carbonLoadModel = createCarbonLoadModel();
        carbonLoadModelList.add(carbonLoadModel);
        context = createTaskContext();
        taskAttemptContextList.add(context);
    }

    @Override
    protected int getRecordWriterNumber(String[] record) {
        return 0;
    }


    private String generateTaskNumber(TaskAttemptContext context, String segmentId) {
        int taskId = context.getTaskAttemptID().getTaskID().getId();
        if(taskId < 0) {
            taskId = -taskId;
        }
        return TaskNumberGenerator.generateUniqueNumber(taskId, segmentId, 6);
    }


    private String updatePartition(CarbonTable carbonTable, String partition) {
        partition = trimPartition(partition);
        Map<String,String> partitionSpec = generatePartitionSpec(partition);
        partitionSpec = CarbonTypeConverter.updatePartitions(partitionSpec, carbonTable);
        List<String> groupList = partitionSpec.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue()).collect(Collectors.toList());
        return StringUtils.join(groupList, "/");
    }


    private List<String> generatePartitionList() {
        partition = partition.trim();
        if(partition.startsWith(ConstantValue.SINGLE_SLASH_SYMBOL)) {
            partition = partition.substring(1);
        }
        if(partition.endsWith(ConstantValue.SINGLE_SLASH_SYMBOL)) {
            partition = partition.substring(0, partition.length() - 1);
        }
        String[] splits = partition.split(ConstantValue.SINGLE_SLASH_SYMBOL);
        return Arrays.asList(splits);
    }

    private String trimPartition(String partition) {
        partition = partition.trim();
        if(partition.startsWith(ConstantValue.SINGLE_SLASH_SYMBOL)) {
            partition = partition.substring(1);
        }
        if(partition.endsWith(ConstantValue.SINGLE_SLASH_SYMBOL)) {
            partition = partition.substring(0, partition.length() - 1);
        }
        return partition;
    }


    private Map<String,String> generatePartitionSpec(String partition) {
        String[] groups = partition.split(ConstantValue.SINGLE_SLASH_SYMBOL);
        Map<String,String> map = new HashMap<>((groups.length<<2)/3);
        for(String group : groups) {
            String[] pair = group.split(ConstantValue.EQUAL_SYMBOL);
            map.put(pair[0], pair[1]);
        }
        return map;
    }


    @Override
    protected void postCloseRecordWriter(int writerNo) throws IOException {
        List<String> partitionList = generatePartitionList();
        SegmentFileStore.writeSegmentFile(carbonLoadModel.getTablePath(),
                taskNumber,
                writePath,
                carbonLoadModel.getSegmentId() + "_" + carbonLoadModel.getFactTimeStamp() + "",
                partitionList
        );

        LoadMetadataDetails newMetaEntry = carbonLoadModel.getCurrentLoadMetadataDetail();
        String readPath = CarbonTablePath.getSegmentFilesLocation(carbonLoadModel.getTablePath())
                + CarbonCommonConstants.FILE_SEPARATOR
                + carbonLoadModel.getSegmentId() + "_" + carbonLoadModel.getFactTimeStamp() + ".tmp";

        String segmentFileName = SegmentFileStore.genSegmentFileName(
                carbonLoadModel.getSegmentId(), String.valueOf(carbonLoadModel.getFactTimeStamp()));
        SegmentFileStore.SegmentFile segmentFile = SegmentFileStore
                .mergeSegmentFiles(readPath, segmentFileName,
                        CarbonTablePath.getSegmentFilesLocation(carbonLoadModel.getTablePath()));
        if (segmentFile != null) {
            if (null == newMetaEntry) {
                throw new RuntimeException("Internal Error");
            }
            // Move all files from temp directory of each segment to partition directory
            SegmentFileStore.moveFromTempFolder(segmentFile,
                    carbonLoadModel.getSegmentId() + "_" + carbonLoadModel.getFactTimeStamp() + ".tmp",
                    carbonLoadModel.getTablePath());
            newMetaEntry.setSegmentFile(segmentFileName + CarbonTablePath.SEGMENT_EXT);
        }

        CarbonLoaderUtil.populateNewLoadMetaEntry(newMetaEntry, SegmentStatus.SUCCESS, carbonLoadModel.getFactTimeStamp(),
                true);

        CarbonLoaderUtil.addDataIndexSizeIntoMetaEntry(newMetaEntry, carbonLoadModel.getSegmentId(), carbonTable);

        CarbonLoaderUtil.recordNewLoadMetadata(newMetaEntry, carbonLoadModel, false, false, "");

        CarbonLoaderUtil.mergeIndexFilesinPartitionedSegment(carbonTable, carbonLoadModel.getSegmentId(), String.valueOf(carbonLoadModel.getFactTimeStamp()));

    }

    @Override
    protected void createRecordWriterList() {
        try {
            RecordWriter recordWriter = createRecordWriter(carbonLoadModel, context);
            recordWriterList.add(recordWriter);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected TaskAttemptContext createTaskContext() {
        Random random = new Random();
        JobID jobId = new JobID(UUID.randomUUID().toString(), 0);
        TaskID task = new TaskID(jobId, TaskType.MAP, random.nextInt());
        TaskAttemptID attemptId = new TaskAttemptID(task, random.nextInt());
        Configuration conf = new Configuration(FileFactory.getConfiguration());
        TaskAttemptContextImpl context = new TaskAttemptContextImpl(conf, attemptId);
        taskNumber = generateTaskNumber(context, carbonLoadModel.getSegmentId());
        context.getConfiguration().set("carbon.outputformat.taskno", taskNumber);
        context.getConfiguration().set("carbon.outputformat.writepath", writePath + "/" + carbonLoadModel.getSegmentId() + "_" + carbonLoadModel.getFactTimeStamp() + ".tmp");
        return context;
    }
}

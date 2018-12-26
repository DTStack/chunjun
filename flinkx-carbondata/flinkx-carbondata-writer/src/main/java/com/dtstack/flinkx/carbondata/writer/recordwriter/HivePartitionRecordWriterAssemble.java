package com.dtstack.flinkx.carbondata.writer.recordwriter;


import com.dtstack.flinkx.carbondata.writer.TaskNumberGenerator;
import com.dtstack.flinkx.carbondata.writer.recordwriter.AbstractRecordWriterAssemble;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;


public class HivePartitionRecordWriterAssemble extends AbstractRecordWriterAssemble {

    private String partition;

    private String taskNumber;

    private String writePath;

    private CarbonLoadModel carbonLoadModel;

    private TaskAttemptContext context;


    public HivePartitionRecordWriterAssemble(CarbonTable carbonTable, String partition) {
        super(carbonTable);
        this.partition = partition;
        writePath = carbonTable.getTablePath() + "/" + partition;
        carbonLoadModel = createCarbonLoadModel();
        carbonLoadModelList.add(carbonLoadModel);
        context = createTaskContext();
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
    protected int getRecordWriterNumber(String[] record) {
        return 0;
    }


    private String generateTaskNumber(TaskAttemptContext context, String segmentId) {
        int taskID = context.getTaskAttemptID().getTaskID().getId();
        if(taskID < 0) {
            taskID = -taskID;
        }
        return TaskNumberGenerator.generateUniqueNumber(taskID, segmentId, 6);
    }


    private List<String> generatePartitionList() {
        partition = partition.trim();
        if(partition.startsWith("/")) {
            partition = partition.substring(1);
        }
        if(partition.endsWith("/")) {
            partition = partition.substring(0, partition.length() - 1);
        }
        String[] splits = partition.split("/");
        return Arrays.asList(splits);
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
    protected TaskAttemptContext createTaskContext() {
        Random random = new Random();
        JobID jobId = new JobID(UUID.randomUUID().toString(), 0);
        TaskID task = new TaskID(jobId, TaskType.MAP, random.nextInt());
        TaskAttemptID attemptID = new TaskAttemptID(task, random.nextInt());
        Configuration conf = new Configuration(FileFactory.getConfiguration());
        TaskAttemptContextImpl context = new TaskAttemptContextImpl(conf, attemptID);
        taskNumber = generateTaskNumber(context, carbonLoadModel.getSegmentId());
        context.getConfiguration().set("carbon.outputformat.taskno", taskNumber);
        context.getConfiguration().set("carbon.outputformat.writepath", writePath + "/" + carbonLoadModel.getSegmentId() + "_" + carbonLoadModel.getFactTimeStamp() + ".tmp");
        return context;
    }


}

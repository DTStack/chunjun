package com.dtstack.flinkx.alluxio.writer;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.outputformat.BaseFileOutputFormat;
import com.dtstack.flinkx.util.ColumnTypeUtil;
import com.dtstack.flinkx.util.SysUtil;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author wuzhongjian_yewu@cmss.chinamobile.com
 * @date 2021-12-02
 */
public abstract class BaseAlluxioOutputFormat extends BaseFileOutputFormat {

    private static final int FILE_NAME_PART_SIZE = 3;

    protected int rowGroupSize;

    protected FileSystem fs;

    protected List<String> columnTypes;

    protected List<String> columnNames;

    protected List<String> fullColumnNames;

    protected List<String> fullColumnTypes;

    protected String delimiter;

    protected int[] colIndices;

    protected Configuration conf;

    protected boolean enableDictionary;

    protected String writeType;

    protected transient Map<String, ColumnTypeUtil.DecimalInfo> decimalColInfo;

    /**
     * 如果key为string类型的值是map 或者 list 会使用gson转为json格式存入
     */
    protected transient Gson gson;

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        gson = new Gson();

        initColIndices();
        super.openInternal(taskNumber, numTasks);
    }

    @Override
    protected void checkOutputDir() {
        try {
            Path dir = new Path(outputFilePath);

            if (fs.exists(dir)) {
                if (fs.getFileStatus(dir).isFile()) {
                    throw new RuntimeException("Can't write new files under common file: " + dir + "\n"
                            + "One can only write new files under directories");
                }
            } else {
                if (!makeDir) {
                    throw new RuntimeException("Output path not exists:" + outputFilePath);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Check output path error", e);
        }
    }

    @Override
    protected void createActionFinishedTag() {
        try {
            if (fs.createNewFile(new Path(actionFinishedTag))) {
                LOG.info("Succeed to create action finished tag:{}", actionFinishedTag);
            } else {
                LOG.warn("Failed to create action finished tag:{}", actionFinishedTag);
            }
        } catch (Exception e) {
            throw new RuntimeException("Create action finished tag error:", e);
        }
    }

    @Override
    protected void waitForActionFinishedBeforeWrite() {
        try {
            Path path = new Path(actionFinishedTag);
            boolean readyWrite = fs.exists(path);
            int n = 0;
            while (!readyWrite) {
                if (n > SECOND_WAIT) {
                    throw new RuntimeException("Wait action finished before write timeout");
                }

                SysUtil.sleep(1000);
                readyWrite = fs.exists(path);
                n++;
            }
        } catch (Exception e) {
            LOG.warn("Call method waitForActionFinishedBeforeWrite error", e);
        }
    }

    @Override
    protected void cleanDirtyData() {
        int fileIndex = formatState.getFileIndex();
        String lastJobId = formatState.getJobId();
        LOG.info("start to cleanDirtyData, fileIndex = {}, lastJobId = {}", fileIndex, lastJobId);
        if (StringUtils.isBlank(lastJobId)) {
            return;
        }

        PathFilter filter = path -> {
            String fileName = path.getName();
            if (!fileName.contains(lastJobId)) {
                return false;
            }

            String[] splits = fileName.split("\\.");
            if (splits.length == FILE_NAME_PART_SIZE) {
                return Integer.parseInt(splits[2]) > fileIndex;
            }

            return false;
        };

        try {
            FileStatus[] dirtyData = fs.listStatus(new Path(outputFilePath), filter);
            if (dirtyData != null && dirtyData.length > 0) {
                for (FileStatus dirtyDatum : dirtyData) {
                    fs.delete(dirtyDatum.getPath(), false);
                    LOG.info("Delete dirty data file:{}", dirtyDatum.getPath());
                }
            }
        } catch (Exception e) {
            LOG.error("Clean dirty data error:", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void openSource() throws IOException {
        try {
            conf = new Configuration();
            conf.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem");
            conf.set("fs.AbstractFileSystem.alluxio.impl", "alluxio.hadoop.AlluxioFileSystem");
            //默认情况下，数据被同步地写入到底层存储系统(hdfs or eos)，但不会被写入到Alluxio的Worker。
            conf.set("alluxio.user.file.writetype.default", writeType);
            fs = new Path(path).getFileSystem(conf);
        } catch (Exception e) {
            LOG.error("Failed to get AlluxioFileSystem with exception : " + e.getMessage());
            throw new RuntimeException("Failed to get AlluxioFileSystem with exception", e);
        }
    }

    private void initColIndices() {
        if (fullColumnNames == null || fullColumnNames.size() == 0) {
            fullColumnNames = columnNames;
        }

        if (fullColumnTypes == null || fullColumnTypes.size() == 0) {
            fullColumnTypes = columnTypes;
        }

        colIndices = new int[fullColumnNames.size()];
        for (int i = 0; i < fullColumnNames.size(); ++i) {
            int j = 0;
            for (; j < columnNames.size(); ++j) {
                if (fullColumnNames.get(i).equalsIgnoreCase(columnNames.get(j))) {
                    colIndices[i] = j;
                    break;
                }
            }
            if (j == columnNames.size()) {
                colIndices[i] = -1;
            }
        }
    }

    @Override
    protected void moveTemporaryDataBlockFileToDirectory() {
        try {
            if (currentBlockFileName != null && currentBlockFileName.startsWith(ConstantValue.POINT_SYMBOL)) {
                Path src = new Path(tmpPath + SP + currentBlockFileName);
                if (!fs.exists(src)) {
                    LOG.warn("block file {} not exists", currentBlockFileName);
                    return;
                }

                String dataFileName = currentBlockFileName.replaceFirst("\\.", "");
                Path dist = new Path(tmpPath + SP + dataFileName);

                if (fs.rename(src, dist)) {
                    LOG.info("Rename temporary data block file:{} to:{}", src, dist);
                } else {
                    LOG.info("Failed to rename temporary data block file:{} to:{}", src, dist);
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to rename file with exception : " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void clearTemporaryDataFiles() throws IOException {
        Path finishedDir, tmpDir;
        if (outputFilePath.endsWith(SP)) {
            finishedDir = new Path(outputFilePath, FINISHED_SUBDIR);
            tmpDir = new Path(outputFilePath, DATA_SUBDIR);
        } else {
            finishedDir = new Path(outputFilePath + SP + FINISHED_SUBDIR);
            tmpDir = new Path(outputFilePath + SP + DATA_SUBDIR);
        }

        if (fs.delete(finishedDir, true)) {
            LOG.info("Succeed to delete .finished dir:{}", finishedDir);
        } else {
            LOG.warn("Failed to delete .finished dir:{}", finishedDir);
        }

        if (fs.delete(tmpDir, true)) {
            LOG.info("Succeed to delete .data dir:{}", tmpDir);
        } else {
            LOG.warn("Failed to delete .data dir:{}", tmpDir);
        }
    }

    @Override
    protected void closeSource() throws IOException {
        if (fs != null) {
            fs.close();
        }
    }

    @Override
    protected void createFinishedTag() throws IOException {
        if (fs != null) {
            fs.createNewFile(new Path(finishedPath));
            LOG.info("Create finished tag dir:{}", finishedPath);
        }
    }

    @Override
    protected void waitForAllTasksToFinish() throws IOException {
        Path finishedDir = new Path(outputFilePath + SP + FINISHED_SUBDIR);
        final int maxRetryTime = 100;
        int i = 0;
        for (; i < maxRetryTime; ++i) {
            if (fs.listStatus(finishedDir).length == numTasks) {
                break;
            }
            SysUtil.sleep(3000);
        }

        if (i == maxRetryTime) {
            String subTaskDataPath = outputFilePath + SP + DATA_SUBDIR;
            fs.delete(new Path(subTaskDataPath), true);
            LOG.info("waitForAllTasksToFinish: delete path:[{}]", subTaskDataPath);

            fs.delete(finishedDir, true);
            LOG.info("waitForAllTasksToFinish: delete finished dir:[{}]", finishedDir);

            throw new RuntimeException("timeout when gathering finish tags for each subtasks");
        }
    }

    @Override
    protected void coverageData() throws IOException {
        LOG.info("Overwrite the original data");

        Path dir = new Path(outputFilePath);
        if (!fs.exists(dir)) {
            return;
        }

        fs.delete(dir, true);
        fs.mkdirs(dir);
    }

    @Override
    protected void moveTemporaryDataFileToDirectory() throws IOException {
        PathFilter pathFilter = path -> path.getName().startsWith(String.valueOf(taskNumber));
        Path dir = new Path(outputFilePath);
        Path tmpDir = new Path(tmpPath);

        FileStatus[] dataFiles = fs.listStatus(tmpDir, pathFilter);
        for (FileStatus dataFile : dataFiles) {
            if (fs.rename(dataFile.getPath(), new Path(dir, dataFile.getPath().getName()))) {
                LOG.info("Rename temp file:{} to dir:{}", dataFile.getPath(), dir);
            } else {
                LOG.warn("Failed to rename temp file:{} to dir:{}", dataFile.getPath(), dir);
            }
        }
    }

    @Override
    protected void moveAllTemporaryDataFileToDirectory() throws IOException {
        PathFilter pathFilter = path -> !path.getName().startsWith(".");
        Path dir = new Path(outputFilePath);
        Path tmpDir = new Path(tmpPath);

        FileStatus[] dataFiles = fs.listStatus(tmpDir, pathFilter);
        for (FileStatus dataFile : dataFiles) {
            if (fs.rename(dataFile.getPath(), new Path(dir, dataFile.getPath().getName()))) {
                LOG.info("Rename temp file:{} to dir:{}", dataFile.getPath(), dir);
            } else {
                LOG.warn("Failed to rename temp file:{} to dir:{}", dataFile.getPath(), dir);
            }
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        notSupportBatchWrite("AlluxioWriter");
    }

}
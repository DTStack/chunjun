/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.core.writer;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.MergedBlockIndex;
import org.apache.carbondata.format.MergedBlockIndexHeader;

import org.apache.hadoop.fs.Path;

public class CarbonIndexFileMergeWriter {

    /**
     * table handle
     */
    private CarbonTable table;

    /**
     * thrift writer object
     */
    private ThriftWriter thriftWriter;

    public CarbonIndexFileMergeWriter(CarbonTable table) {
        this.table = table;
    }

    /**
     * Merge all the carbonindex files of segment to a  merged file
     * @param tablePath
     * @param indexFileNamesTobeAdded while merging it comsiders only these files.
     *                                If null then consider all
     * @param readFileFooterFromCarbonDataFile flag to read file footer information from carbondata
     *                                         file. This will used in case of upgrade from version
     *                                         which do not store the blocklet info to current version
     * @throws IOException
     */
    private String mergeCarbonIndexFilesOfSegment(String segmentId,
                                                  String tablePath, List<String> indexFileNamesTobeAdded,
                                                  boolean readFileFooterFromCarbonDataFile, String uuid) throws IOException {
        Segment segment = Segment.getSegment(segmentId, tablePath);
        String segmentPath = CarbonTablePath.getSegmentPath(tablePath, segmentId);
        CarbonFile[] indexFiles;
        SegmentFileStore sfs = null;
        if (segment != null && segment.getSegmentFileName() != null) {
            sfs = new SegmentFileStore(tablePath, segment.getSegmentFileName());
            List<CarbonFile> indexCarbonFiles = sfs.getIndexCarbonFiles();
            indexFiles = indexCarbonFiles.toArray(new CarbonFile[indexCarbonFiles.size()]);
        } else {
            indexFiles = SegmentIndexFileStore.getCarbonIndexFiles(segmentPath);
        }
        if (isCarbonIndexFilePresent(indexFiles) || indexFileNamesTobeAdded != null) {
            if (sfs == null) {
                return writeMergeIndexFileBasedOnSegmentFolder(indexFileNamesTobeAdded,
                        readFileFooterFromCarbonDataFile, segmentPath, indexFiles, segmentId);
            } else {
                return writeMergeIndexFileBasedOnSegmentFile(
                        segmentId, indexFileNamesTobeAdded, sfs, indexFiles, uuid);
            }
        }
        return null;
    }

    private String writeMergeIndexFileBasedOnSegmentFolder(List<String> indexFileNamesTobeAdded,
                                                           boolean readFileFooterFromCarbonDataFile, String segmentPath, CarbonFile[] indexFiles,
                                                           String segmentId) throws IOException {
        SegmentIndexFileStore fileStore = new SegmentIndexFileStore();
        if (readFileFooterFromCarbonDataFile) {
            // this case will be used in case of upgrade where old store will not have the blocklet
            // info in the index file and therefore blocklet info need to be read from the file footer
            // in the carbondata file
            fileStore.readAllIndexAndFillBolckletInfo(segmentPath);
        } else {
            fileStore.readAllIIndexOfSegment(segmentPath);
        }
        Map<String, byte[]> indexMap = fileStore.getCarbonIndexMap();
        writeMergeIndexFile(indexFileNamesTobeAdded, segmentPath, indexMap, segmentId);
        for (CarbonFile indexFile : indexFiles) {
            indexFile.delete();
        }
        return null;
    }

    private String writeMergeIndexFileBasedOnSegmentFile(String segmentId,
                                                         List<String> indexFileNamesTobeAdded, SegmentFileStore segmentFileStore,
                                                         CarbonFile[] indexFiles, String uuid) throws IOException {
        SegmentIndexFileStore fileStore = new SegmentIndexFileStore();
        fileStore
                .readAllIIndexOfSegment(segmentFileStore.getSegmentFile(), segmentFileStore.getTablePath(),
                        SegmentStatus.SUCCESS, true);
        Map<String, byte[]> indexMap = fileStore.getCarbonIndexMapWithFullPath();
        Map<String, Map<String, byte[]>> indexLocationMap = new HashMap<>();
        for (Map.Entry<String, byte[]> entry: indexMap.entrySet()) {
            Path path = new Path(entry.getKey());
            Map<String, byte[]> map = indexLocationMap.get(path.getParent().toString());
            if (map == null) {
                map = new HashMap<>();
                indexLocationMap.put(path.getParent().toString(), map);
            }
            map.put(path.getName(), entry.getValue());
        }
        for (Map.Entry<String, Map<String, byte[]>> entry : indexLocationMap.entrySet()) {
            String mergeIndexFile =
                    writeMergeIndexFile(indexFileNamesTobeAdded, entry.getKey(), entry.getValue(), segmentId);
            for (Map.Entry<String, SegmentFileStore.FolderDetails> segentry : segmentFileStore
                    .getLocationMap().entrySet()) {
                String location = segentry.getKey();
                if (segentry.getValue().isRelative()) {
                    location =
                            segmentFileStore.getTablePath() + CarbonCommonConstants.FILE_SEPARATOR + location;
                }
                if (new Path(entry.getKey()).equals(new Path(location))) {
                    segentry.getValue().setMergeFileName(mergeIndexFile);
                    segentry.getValue().setFiles(new HashSet<String>());
                    break;
                }
            }
        }
        String newSegmentFileName = SegmentFileStore.genSegmentFileName(segmentId, uuid)
                + CarbonTablePath.SEGMENT_EXT;
        String path = CarbonTablePath.getSegmentFilesLocation(table.getTablePath())
                + CarbonCommonConstants.FILE_SEPARATOR + newSegmentFileName;
        long t1 = System.currentTimeMillis();
        SegmentFileStore.writeSegmentFile(segmentFileStore.getSegmentFile(), path);
        long t2 = System.currentTimeMillis();
        System.out.println("baga1_time: " + (t2 - t1));
        SegmentFileStore.updateSegmentFile(table, segmentId, newSegmentFileName,
                table.getCarbonTableIdentifier().getTableId(), segmentFileStore);
        long t3 = System.currentTimeMillis();
        System.out.println("baga2_time: " + (t3 - t2));
        for (CarbonFile file : indexFiles) {
            file.delete();
        }

        return uuid;
    }

    private String writeMergeIndexFile(List<String> indexFileNamesTobeAdded, String segmentPath,
                                       Map<String, byte[]> indexMap, String segment_id) throws IOException {
        MergedBlockIndexHeader indexHeader = new MergedBlockIndexHeader();
        MergedBlockIndex mergedBlockIndex = new MergedBlockIndex();
        List<String> fileNames = new ArrayList<>(indexMap.size());
        List<ByteBuffer> data = new ArrayList<>(indexMap.size());
        for (Map.Entry<String, byte[]> entry : indexMap.entrySet()) {
            if (indexFileNamesTobeAdded == null ||
                    indexFileNamesTobeAdded.contains(entry.getKey())) {
                fileNames.add(entry.getKey());
                data.add(ByteBuffer.wrap(entry.getValue()));
            }
        }
        if (fileNames.size() > 0) {
            String mergeIndexName =
                    segment_id + '_' + System.currentTimeMillis() + CarbonTablePath.MERGE_INDEX_FILE_EXT;
            openThriftWriter(segmentPath + "/" + mergeIndexName);
            indexHeader.setFile_names(fileNames);
            mergedBlockIndex.setFileData(data);
            writeMergedBlockIndexHeader(indexHeader);
            writeMergedBlockIndex(mergedBlockIndex);
            close();
            return mergeIndexName;
        }
        return null;
    }

    /**
     * Merge all the carbonindex files of segment to a  merged file
     *
     * @param segmentId
     * @throws IOException
     */
    public String mergeCarbonIndexFilesOfSegment(String segmentId, String uuid,
                                                 String tablePath) throws IOException {
        return mergeCarbonIndexFilesOfSegment(segmentId, tablePath, null, false, uuid);
    }

    /**
     * Merge all the carbonindex files of segment to a  merged file
     *
     * @param segmentId
     * @param readFileFooterFromCarbonDataFile
     * @throws IOException
     */
    public String mergeCarbonIndexFilesOfSegment(String segmentId,
                                                 String tablePath, boolean readFileFooterFromCarbonDataFile, String uuid) throws IOException {
        return mergeCarbonIndexFilesOfSegment(segmentId, tablePath, null,
                readFileFooterFromCarbonDataFile, uuid);
    }

    private boolean isCarbonIndexFilePresent(CarbonFile[] indexFiles) {
        for (CarbonFile file : indexFiles) {
            if (file.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
                return true;
            }
        }
        return false;
    }

    /**
     * It writes thrift object to file
     *
     * @throws IOException
     */
    private void writeMergedBlockIndexHeader(MergedBlockIndexHeader indexObject) throws IOException {
        thriftWriter.write(indexObject);
    }

    /**
     * It writes thrift object to file
     *
     * @throws IOException
     */
    private void writeMergedBlockIndex(MergedBlockIndex indexObject) throws IOException {
        thriftWriter.write(indexObject);
    }

    /**
     * Below method will be used to open the thrift writer
     *
     * @param filePath file path where data need to be written
     * @throws IOException throws io exception in case of any failure
     */
    private void openThriftWriter(String filePath) throws IOException {
        // create thrift writer instance
        thriftWriter = new ThriftWriter(filePath, false);
        // open the file stream
        thriftWriter.open(FileWriteOperation.OVERWRITE);
    }

    /**
     * Below method will be used to close the thrift object
     */
    private void close() throws IOException {
        thriftWriter.close();
    }

    public static class SegmentIndexFIleMergeStatus implements Serializable {

        private SegmentFileStore.SegmentFile segmentFile;

        private List<String> filesTobeDeleted;

        public SegmentIndexFIleMergeStatus(SegmentFileStore.SegmentFile segmentFile,
                                           List<String> filesTobeDeleted) {
            this.segmentFile = segmentFile;
            this.filesTobeDeleted = filesTobeDeleted;
        }

        public SegmentFileStore.SegmentFile getSegmentFile() {
            return segmentFile;
        }

        public List<String> getFilesTobeDeleted() {
            return filesTobeDeleted;
        }
    }
}

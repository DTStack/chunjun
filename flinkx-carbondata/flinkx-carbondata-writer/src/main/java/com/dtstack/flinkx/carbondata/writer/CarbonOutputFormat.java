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
package com.dtstack.flinkx.carbondata.writer;


import com.dtstack.flinkx.carbondata.CarbondataUtil;
import com.dtstack.flinkx.carbondata.writer.dict.CarbonTypeConverter;
import com.dtstack.flinkx.carbondata.writer.recordwriter.AbstractRecordWriter;
import com.dtstack.flinkx.carbondata.writer.recordwriter.RecordWriterFactory;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.util.DateUtil;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Carbondata Output Format
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class CarbonOutputFormat extends BaseRichOutputFormat implements CleanupWhenUnsuccessful {

    protected Map<String,String> hadoopConfig;

    protected String defaultFs;

    protected String table;

    protected String database;

    protected String path;

    protected List<String> column;

    protected boolean overwrite = false;

    protected String partition;

    protected int batchSize;

    private int numWrites = 0;

    private CarbonTable carbonTable;

    private AbstractRecordWriter recordWriterAssemble;

    private List<String> fullColumnNames;

    private List<DataType> fullColumnTypes;

    private List<Integer> fullColumnIndices;

    private static final String SLASH = "/";

    private static final String ASSIGN = "=";

    private boolean isHivePartitioned = false;

    private List<Integer> partitionColIndex = new ArrayList<>();

    private List<String> partitionColValue = new ArrayList<>();

    private final String DEFAULT_SERIAL_NULL_FORMAT = "\\N";

    private final List<String> oldSegmentIds = new ArrayList<>();


    @Override
    public void configure(Configuration parameters) {
        CarbondataUtil.initFileFactory(hadoopConfig, defaultFs);
    }

    private void parsePartition(){
        if(partition == null || partition.trim().length() == 0) {
            throw new IllegalArgumentException("The table have partition field，'partition' should not be empty");
        }

        partition = partition.trim();
        if(partition.startsWith(SLASH)) {
            partition = partition.substring(1);
        }

        if(partition.endsWith(SLASH)) {
            partition = partition.substring(0, partition.length() - 1);
        }

        String[] splits = partition.split(SLASH);

        Preconditions.checkArgument(splits.length == carbonTable.getPartitionInfo().getColumnSchemaList().size());

        for(String split : splits) {
            String[] pair = split.split(ASSIGN);
            String name = pair[0];
            String val = pair[1];
            partitionColIndex.add(fullColumnNames.indexOf(name));
            partitionColValue.add(val);
        }

    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {
        this.taskNumber = taskNumber;
        this.numTasks = numTasks;

        carbonTable = CarbondataUtil.buildCarbonTable(database, table, path);

        isHivePartitioned = carbonTable.isHivePartitionTable();

        inferFullColumnInfo();

        if(isHivePartitioned) {
            parsePartition();
        }

        recordWriterAssemble = RecordWriterFactory.getAssembleInstance(carbonTable, partition);

    }


    private void inferFullColumnInfo() {
        fullColumnIndices = new ArrayList<>();
        fullColumnNames = new ArrayList<>();
        fullColumnTypes = new ArrayList<>();

        column = column.stream().map(String::toLowerCase).collect(Collectors.toList());

        List<ColumnSchema> columnSchemas = carbonTable.getTableInfo().getFactTable().getListOfColumns();
        for(int i = 0; i < columnSchemas.size(); ++i) {
            ColumnSchema columnSchema = columnSchemas.get(i);
            if(!columnSchema.isInvisible()) {
                fullColumnNames.add(columnSchema.getColumnName());
                fullColumnTypes.add(columnSchema.getDataType());
            }
        }

        for(int i = 0; i < fullColumnNames.size(); ++i) {
            fullColumnIndices.add(column.indexOf(fullColumnNames.get(i)));
        }
    }


    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        int i = 0;
        try {
            String[] record = new String[fullColumnNames.size()];
            for(; i < fullColumnIndices.size(); ++i) {
                int index = fullColumnIndices.get(i);
                if(index == -1) {
                    record[i] = DEFAULT_SERIAL_NULL_FORMAT;
                } else {
                    Object column = row.getField(index);
                    String s = CarbonTypeConverter.objectToString(column, DEFAULT_SERIAL_NULL_FORMAT);
                    CarbonTypeConverter.checkStringType(s, DEFAULT_SERIAL_NULL_FORMAT, fullColumnTypes.get(i));
                    record[i] = s;
                }
            }
            if(isHivePartitioned) {
                for(int j = 0; j < partitionColIndex.size(); ++j) {
                    int index = partitionColIndex.get(j);
                    if(index != -1) {
                        record[index] = partitionColValue.get(j);
                    }
                }
            }
            recordWriterAssemble.write(record);
        } catch(Exception e) {
            if(i < row.getArity()) {
                throw new WriteRecordException(recordConvertDetailErrorMessage(i, row), e, i, row);
            }
            throw new WriteRecordException(e.getMessage(), e);
        }
        numWrites++;
        if(numWrites == batchSize) {
            try {
                closeInternal();
                openInternal(taskNumber, numTasks);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            numWrites = 0;
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        // CAN NOT HAPPEN
        throw new IllegalArgumentException("It can not happen.");
    }

    @Override
    public void tryCleanupOnError() throws Exception {
        // try clean up on error
    }

    @Override
    protected boolean needWaitBeforeOpenInternal() {
        return overwrite;
    }

    @Override
    protected void beforeOpenInternal() {
        if(taskNumber == 0 && overwrite) {
            String metaPath = path + SLASH + "/Metadata";
            LoadMetadataDetails[] loadMetadataDetailsArr = SegmentStatusManager.readLoadMetadata(metaPath);
            if(loadMetadataDetailsArr != null) {
                for(LoadMetadataDetails loadMetadataDetails : loadMetadataDetailsArr) {
                    if(loadMetadataDetails.getSegmentStatus() == SegmentStatus.SUCCESS) {
                        oldSegmentIds.add(loadMetadataDetails.getLoadName());
                    }
                }
            }
        }
    }

    @Override
    public void closeInternal() throws IOException {
        if(recordWriterAssemble != null) {
            try {
                recordWriterAssemble.close();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected boolean needWaitAfterCloseInternal() {
        return overwrite;
    }

    @Override
    protected void afterCloseInternal()  {
        if(taskNumber == 0 && overwrite) {
            if(!oldSegmentIds.isEmpty()) {
                String metaPath = path + SLASH + "/Metadata";
                try {
                    List<String> invalidLoadIds = SegmentStatusManager.updateDeletionStatus(carbonTable.getAbsoluteTableIdentifier(), oldSegmentIds, metaPath);
                    if(invalidLoadIds.isEmpty()) {
                        LOG.info("Delete segment by Id is successfull");
                    } else {
                        LOG.error("Delete segment by Id is failed");
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

}

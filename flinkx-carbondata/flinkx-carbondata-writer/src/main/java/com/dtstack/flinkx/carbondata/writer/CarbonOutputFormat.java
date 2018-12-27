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
import com.dtstack.flinkx.carbondata.writer.recordwriter.AbstractRecordWriterAssemble;
import com.dtstack.flinkx.carbondata.writer.recordwriter.RecordWriterAssembleFactory;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.processing.loading.TableProcessingOperations;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
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
public class CarbonOutputFormat extends RichOutputFormat implements CleanupWhenUnsuccessful {

    protected Map<String,String> hadoopConfig;

    protected String defaultFS;

    protected String table;

    protected String database;

    protected String path;

    protected List<String> column;

    protected boolean overwrite = false;

    protected String partition;

    private CarbonTable carbonTable;

    private AbstractRecordWriterAssemble recordWriterAssemble;

    private List<String> fullColumnNames;

    private List<DataType> fullColumnTypes;

    private List<Integer> fullColumnIndices;

    private String bakPath;

    private FileSystem fs;

    private static final String SLASH = "/";

    private static final String ASSIGN = "=";

    private boolean isHivePartitioned = false;

    private List<Integer> partitionColIndex = new ArrayList<>();

    private List<String> partitionColValue = new ArrayList<>();

    private final SimpleDateFormat DEFAULT_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    private final SimpleDateFormat DEFAULT_TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private final String DEFAULT_SERIAL_NULL_FORMAT = "\\N";


    @Override
    public void configure(Configuration parameters) {
        try {
            CarbondataUtil.initFileFactory(hadoopConfig, defaultFS);
            fs = FileSystem.get(FileFactory.getConfiguration());
            bakPath = path + "_bak";
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void parsePartition(){
        if(carbonTable.getPartitionInfo() == null) {
            return;
        }

        if(partition == null || partition.trim().length() == 0) {
            return;
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
        if(StringUtils.isBlank(partition) && overwrite) {
            carbonTable = CarbondataUtil.buildCarbonTable(database, table, bakPath);
        } else {
            carbonTable = CarbondataUtil.buildCarbonTable(database, table, path);
        }

        isHivePartitioned = carbonTable.isHivePartitionTable();

        inferFullColumnInfo();

        if(isHivePartitioned) {
            parsePartition();
        }

        TableProcessingOperations.deletePartialLoadDataIfExist(carbonTable, isHivePartitioned);
        SegmentStatusManager.deleteLoadsAndUpdateMetadata(carbonTable, false, null);

        recordWriterAssemble = RecordWriterAssembleFactory.getAssembleInstance(carbonTable, partition);

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
                    String s = CarbonTypeConverter.objectToString(column, DEFAULT_SERIAL_NULL_FORMAT, DEFAULT_TIME_FORMAT, DEFAULT_DATE_FORMAT);
                    CarbonTypeConverter.checkStringType(s, DEFAULT_SERIAL_NULL_FORMAT, DEFAULT_TIME_FORMAT, DEFAULT_DATE_FORMAT, fullColumnTypes.get(i));
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
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        // CAN NOT HAPPEN
        throw new IllegalArgumentException("It can not happen.");
    }

    @Override
    public void tryCleanupOnError() throws Exception {
        if(!isHivePartitioned) {
            fs.delete(new Path(bakPath), true);
        }
    }

    @Override
    protected boolean needWaitBeforeOpenInternal() {
        return overwrite && StringUtils.isBlank(partition);
    }

    @Override
    protected void beforeOpenInternal() {
        if(taskNumber == 0) {
            String schemaPath =  path + "/Metadata/schema";
            InputStream in = null;
            OutputStream out = null;
            try {
                if(fs.exists(new Path(bakPath))) {
                    fs.delete(new Path(bakPath), true);
                }
                fs.mkdirs(new Path(bakPath));
                out = fs.create(new Path(bakPath + "/Metadata/schema"));
                in = fs.open(new Path(schemaPath));
                IOUtils.copyBytes(in, out, 1024);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                if(in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                if(out != null) {
                    try {
                        out.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
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
        return overwrite && StringUtils.isBlank(partition);
    }

    @Override
    protected void afterCloseInternal()  {
        if(taskNumber == 0 && overwrite) {
            try {
                fs.delete(new Path(path), true);
                fs.rename(new Path(bakPath), new Path(path));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


}

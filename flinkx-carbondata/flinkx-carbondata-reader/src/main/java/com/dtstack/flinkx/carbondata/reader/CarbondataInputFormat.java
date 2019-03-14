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
package com.dtstack.flinkx.carbondata.reader;


import com.dtstack.flinkx.carbondata.CarbondataUtil;
import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonProjection;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;


/**
 * Carbondata InputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class CarbondataInputFormat extends RichInputFormat{

    protected Map<String,String> hadoopConfig;

    protected String defaultFS;

    protected String table;

    protected String database;

    protected String path;

    protected List<String> columnValue;

    protected List<String> columnType;

    protected List<String> columnName;

    protected List<String> columnFormat;

    protected String filter;

    private List<Integer> columnIndex;

    private List<String> fullColumnNames;

    private List<DataType> fullColumnTypes;

    private transient Job job;

    private List<CarbonInputSplit> carbonInputSplits;

    private int pos = 0;

    private transient RecordReader recordReader;

    private transient CarbonTableInputFormat format;

    private transient TaskAttemptContext taskAttemptContext;

    private transient CarbonProjection  projection;


    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        CarbonFlinkInputSplit carbonFlinkInputSplit = (CarbonFlinkInputSplit) inputSplit;
        carbonInputSplits = carbonFlinkInputSplit.getCarbonInputSplits();
        taskAttemptContext = createTaskContext();
        try {
            recordReader = createRecordReader(pos);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void initColumnIndices() {
        projection = new CarbonProjection();
        columnIndex = new ArrayList<>();
        int k = 0;
        for(int i = 0; i < columnName.size(); ++i) {
            if(StringUtils.isNotBlank(columnName.get(i))) {
                columnIndex.add(k);
                projection.addColumn(columnName.get(i));
                k++;
            } else {
                columnIndex.add(null);
            }
        }
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        try {
            row = new Row(columnIndex.size());

            Object[] record = (Object[]) recordReader.getCurrentValue();
            for(int i = 0; i < columnIndex.size(); ++i) {
                if(columnIndex == null) {
                    row.setField(i, StringUtil.string2col(columnValue.get(i), columnType.get(i),null));
                } else {
                    row.setField(i, record[columnIndex.get(i)]);
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return row;
    }

    @Override
    protected void closeInternal() throws IOException {
        if(recordReader != null) {
            recordReader.close();
        }
    }

    @Override
    public void configure(Configuration configuration) {
        CarbondataUtil.initFileFactory(hadoopConfig, defaultFS);
        initColumnIndices();
        org.apache.hadoop.conf.Configuration conf = FileFactory.getConfiguration();
        CarbonTableInputFormat.setDatabaseName(conf, database);
        CarbonTableInputFormat.setTableName(conf, table);
        CarbonTableInputFormat.setColumnProjection(conf, projection);

        conf.set("mapreduce.input.fileinputformat.inputdir", path);

        try {
            inferFullColumnInfo();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if(StringUtils.isNotBlank(filter)) {
            CarbonTableInputFormat.setFilterPredicates(conf, CarbonExpressUtil.eval(filter, fullColumnNames, fullColumnTypes));
        }

        try {
            job = Job.getInstance(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        format = new CarbonTableInputFormat();

    }

    @Override
    public InputSplit[] createInputSplits(int num) throws IOException {
        List<org.apache.hadoop.mapreduce.InputSplit> splitList = format.getSplits(job);
        int splitNum = (splitList.size() < num ? splitList.size() : num);
        int groupSize = (int)Math.ceil(splitList.size() / (double)splitNum);
        InputSplit[] ret = new InputSplit[splitNum];

        for(int i = 0; i < splitNum; ++i) {
            List<CarbonInputSplit> carbonInputSplits = new ArrayList<>();
            for(int j = 0; j < groupSize && i*groupSize+j < splitList.size(); ++j) {
                carbonInputSplits.add((CarbonInputSplit) splitList.get(i*groupSize+j));
            }
            ret[i] = new CarbonFlinkInputSplit(carbonInputSplits, i);
        }

        return ret;
    }


    @Override
    public boolean reachedEnd() throws IOException {
        try {
            while(!recordReader.nextKeyValue()) {
                pos++;
                if(pos == carbonInputSplits.size()) {
                    return true;
                }
                recordReader.close();
                recordReader = createRecordReader(pos);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    private TaskAttemptContext createTaskContext() {
        Random random = new Random();
        JobID jobId = new JobID(UUID.randomUUID().toString(), 0);
        TaskID task = new TaskID(jobId, TaskType.MAP, random.nextInt());
        TaskAttemptID attemptID = new TaskAttemptID(task, random.nextInt());
        TaskAttemptContextImpl context = new TaskAttemptContextImpl(job.getConfiguration(), attemptID);
        return context;
    }

    private RecordReader createRecordReader(int pos) throws IOException, InterruptedException {
        CarbonInputSplit carbonInputSplit = carbonInputSplits.get(pos);
        RecordReader recordReader = format.createRecordReader(carbonInputSplit, taskAttemptContext);
        recordReader.initialize(carbonInputSplit, taskAttemptContext);
        return recordReader;
    }


    private void inferFullColumnInfo() throws IOException {
        CarbonTable carbonTable = CarbondataUtil.buildCarbonTable(database, table, path);
        fullColumnNames = new ArrayList<>();
        fullColumnTypes = new ArrayList<>();

        List<ColumnSchema> columnSchemas = carbonTable.getTableInfo().getFactTable().getListOfColumns();
        for(int i = 0; i < columnSchemas.size(); ++i) {
            ColumnSchema columnSchema = columnSchemas.get(i);
            if(!columnSchema.isInvisible()) {
                fullColumnNames.add(columnSchema.getColumnName());
                fullColumnTypes.add(columnSchema.getDataType());
            }
        }
    }

}

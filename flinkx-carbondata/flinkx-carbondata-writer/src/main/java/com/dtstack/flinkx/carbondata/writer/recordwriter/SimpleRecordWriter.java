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


import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;

/**
 * record writer for simple carbon table
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class SimpleRecordWriter extends AbstractRecordWriter {

    public SimpleRecordWriter(CarbonTable carbonTable) {
        super(carbonTable);
        CarbonLoadModel carbonLoadModel = createCarbonLoadModel();
        carbonLoadModelList.add(carbonLoadModel);
        TaskAttemptContext context = createTaskContext();
        taskAttemptContextList.add(context);

    }

    @Override
    protected int getRecordWriterNumber(String[] record) {
        return 0;
    }

    @Override
    protected void createRecordWriterList() {
        RecordWriter recordWriter = null;
        try {
            recordWriter = createRecordWriter(carbonLoadModelList.get(0), taskAttemptContextList.get(0));
            recordWriterList.add(recordWriter);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}

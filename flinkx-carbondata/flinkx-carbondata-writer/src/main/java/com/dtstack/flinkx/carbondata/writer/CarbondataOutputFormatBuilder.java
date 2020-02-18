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

import com.dtstack.flinkx.outputformat.BaseRichOutputFormatBuilder;
import com.dtstack.flinkx.writer.WriteMode;
import org.apache.flink.util.Preconditions;
import java.util.List;
import java.util.Map;

/**
 * The Builder class of CarbondataOutputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class CarbondataOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private CarbonOutputFormat format;

    public CarbondataOutputFormatBuilder() {
        super.format = format = new CarbonOutputFormat();
    }

    public void setHadoopConfig(Map<String,String> hadoopConfig) {
        format.hadoopConfig = hadoopConfig;
    }

    public void setTable(String table) {
        format.table = table;
    }

    public void setDefaultFs(String defaultFs) {
        format.defaultFs = defaultFs;
    }

    public void setPath(String path) {
        format.path = path;
    }

    public void setDatabase(String database) {
        format.database = database;
    }

    public void setColumn(List<String> column) {
        format.column = column;
    }

    public void setWriteMode(String writeMode) {
        if(WriteMode.OVERWRITE.getMode().equalsIgnoreCase(writeMode)) {
            format.overwrite = true;
        } else {
            format.overwrite = false;
        }
    }

    public void setPartition(String partition) {
        format.partition = partition;
    }

    public void setBatchSize(int batchSize) {
        format.batchSize = batchSize;
    }


    @Override
    protected void checkFormat() {
        Preconditions.checkNotNull(format.hadoopConfig);
        Preconditions.checkNotNull(format.table);
        Preconditions.checkNotNull(format.path);
        Preconditions.checkNotNull(format.database);
        Preconditions.checkNotNull(format.column);

        if (format.getRestoreConfig() != null && format.getRestoreConfig().isRestore()){
            throw new UnsupportedOperationException("This plugin not support restore from failed state");
        }
    }
}

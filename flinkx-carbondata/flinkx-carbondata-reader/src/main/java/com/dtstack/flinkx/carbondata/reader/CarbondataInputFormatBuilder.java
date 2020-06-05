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

import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import org.apache.flink.util.Preconditions;
import java.util.List;
import java.util.Map;


/**
 * Carbondata InputFormat Builder
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class CarbondataInputFormatBuilder extends BaseRichInputFormatBuilder {

    private CarbondataInputFormat format;

    public CarbondataInputFormatBuilder() {
        super.format = format = new CarbondataInputFormat();
    }

    public void setHadoopConfig(Map<String,String> hadoopConfig) {
        format.hadoopConfig = hadoopConfig;
    }

    public void setTable(String table) {
        format.table = table;
    }

    public void setPath(String path) {
        format.path = path;
    }

    public void setDatabase(String database) {
        format.database = database;
    }

    public void setColumnNames(List<String> columnNames) {
        format.columnName = columnNames;
    }

    public void setColumnValues(List<String> columnValues) {
        format.columnValue = columnValues;
    }

    public void setColumnTypes(List<String> columnTypes) {
        format.columnType = columnTypes;
    }

    public void setFilter(String filter) {
        format.filter = filter;
    }

    public void setDefaultFs(String defaultFs) {
        format.defaultFs = defaultFs;
    }

    @Override
    protected void checkFormat() {
        Preconditions.checkNotNull(format.hadoopConfig);
        Preconditions.checkNotNull(format.table);
        Preconditions.checkNotNull(format.path);
        Preconditions.checkNotNull(format.database);
        Preconditions.checkNotNull(format.columnName);
        Preconditions.checkNotNull(format.columnType);
        Preconditions.checkNotNull(format.columnValue);
        Preconditions.checkArgument(format.columnName.size() == format.columnType.size());
        Preconditions.checkArgument(format.columnName.size() == format.columnValue.size());

        if (format.getRestoreConfig() != null && format.getRestoreConfig().isRestore()){
            throw new UnsupportedOperationException("This plugin not support restore from failed state");
        }
    }

}

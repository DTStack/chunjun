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

package com.dtstack.flinkx.odps.writer;

import com.dtstack.flinkx.odps.OdpsConfigKeys;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormatBuilder;
import org.apache.commons.lang.StringUtils;
import java.util.Map;

/**
 * The builder class of OdpsOutputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class OdpsOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private OdpsOutputFormat format;

    public OdpsOutputFormatBuilder() {
        super.format = format = new OdpsOutputFormat();
    }

    public void setOdpsConfig(Map<String,String> odpsConfig) {
        format.odpsConfig = odpsConfig;
        format.projectName = odpsConfig.get(OdpsConfigKeys.KEY_PROJECT);
    }

    public void setColumnTypes(String[] columnTypes) {
        format.columnTypes = columnTypes;
    }

    public void setColumnNames(String[] columnNames) {
        format.columnNames = columnNames;
    }


    public void setProjectName(String projectName) {
        format.projectName = projectName;
    }

    public void setTableName(String tableName) {
        format.tableName = tableName;
    }

    public void setPartition(String partition) {
        format.partition = partition;
    }

    public void setWriteMode(String writeMode) {
        this.format.writeMode = StringUtils.isBlank(writeMode) ? "APPEND" : writeMode.toUpperCase();
    }

    public void setBufferSize(long bufferSize){
        format.bufferSize = bufferSize;
    }

    @Override
    protected void checkFormat() {
        if (format.getRestoreConfig() != null && format.getRestoreConfig().isRestore()){
            throw new UnsupportedOperationException("This plugin not support restore from failed state");
        }
    }
}

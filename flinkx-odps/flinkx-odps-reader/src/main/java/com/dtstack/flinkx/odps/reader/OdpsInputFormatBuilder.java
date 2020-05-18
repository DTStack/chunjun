/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.odps.reader;

import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import com.dtstack.flinkx.reader.MetaColumn;

import java.util.List;
import java.util.Map;
import static com.dtstack.flinkx.odps.OdpsConfigKeys.*;

/**
 * The Builder of OdpsInputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class OdpsInputFormatBuilder extends BaseRichInputFormatBuilder {

    private OdpsInputFormat format;

    public OdpsInputFormatBuilder() {
        super.format = format = new OdpsInputFormat();
    }

    public void setOdpsConfig(Map<String,String> odpsConfig) {
        format.odpsConfig = odpsConfig;
        format.projectName = odpsConfig.get(KEY_PROJECT);
    }

    public void setTableName(String tableName) {
        format.tableName = tableName;
    }

    public void setMetaColumn(List<MetaColumn> metaColumns){
        format.metaColumns = metaColumns;
    }

    public void setPartition(String partition) {
        format.partition = partition;
    }

    @Override
    protected void checkFormat() {
        if (format.getRestoreConfig() != null && format.getRestoreConfig().isRestore()){
            throw new UnsupportedOperationException("This plugin not support restore from failed state");
        }
    }


}

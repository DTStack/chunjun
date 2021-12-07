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
package com.dtstack.flinkx.hudi.writer;

import com.dtstack.flinkx.outputformat.BaseRichOutputFormatBuilder;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.avro.Schema;

import java.util.List;
import java.util.Map;

/**
 * @author fengjiangtao_yewu@cmss.chinamobile.com
 * @date 2021-08-10
 */

public class HudiOutputformatBuilder extends BaseRichOutputFormatBuilder {
    private HudiOutputFormat format;

    public HudiOutputformatBuilder() {
        super.format = format = new HudiOutputFormat();
    }

    public void setTableName(String tableName) {
        format.tableName = tableName;
    }

    public void setTableType(String tableType) {
        format.tableType = tableType;
    }
    public void setRecordKey(String recordKey) {
        format.recordKey = recordKey;
    }

    public void setPath(String path) {
        format.path = path;
    }

    public void setHadoopConf(Map<String, Object> hadoopConf) {
        format.hadoopConfig = hadoopConf;
    }

    public void setDefaultFS(String defaultFS) {
        format.defaultFS = defaultFS;
    }

    public void setHiveJdbcUrl(String hiveJdbcUrl) {
        format.hiveJdbcUrl = hiveJdbcUrl;
    }

    public void setHiveMetastore(String hiveMetastore) {
        format.hiveMetastore = hiveMetastore;
    }

    public void setHiveUser(String hiveUser) {
        format.hiveUser = hiveUser;
    }

    public void setHivePass(String hivePass) {
        format.hivePass = hivePass;
    }

    public void setSchema(String schema) {
        format.schema = schema;
    }

    public void setColumns(List<MetaColumn> metaColumns) {
        format.metaColumns = metaColumns;
    }

    public void setPartitionFields(List<String> partitionFields) {
        format.partitionFields = partitionFields;
    }

    /**
     * Column type comes form org.apache.avro.Schema.Type
     */
    @Override
    protected void checkFormat() {
        MetaColumn mColumn = null;
        try {
            for (MetaColumn metaColumn : format.metaColumns) {
                mColumn = metaColumn;
                Schema.Type.valueOf(metaColumn.getType().toUpperCase());
            }
        } catch (Exception e) {
            throw new UnsupportedOperationException("This plugin column's type not support: " + mColumn.getType());
        }
    }
}

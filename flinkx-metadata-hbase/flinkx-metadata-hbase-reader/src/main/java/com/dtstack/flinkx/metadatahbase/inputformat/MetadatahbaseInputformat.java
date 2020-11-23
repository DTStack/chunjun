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

package com.dtstack.flinkx.metadatahbase.inputformat;

import com.dtstack.flinkx.metadata.inputformat.BaseMetadataInputFormat;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @author kunni@dtstack.com
 */
public class MetadatahbaseInputformat extends BaseMetadataInputFormat {

    private static final long serialVersionUID = 1L;

    /**
     * 用于连接hbase的配置
     */
    protected Map<String, Object> hadoopConfig;

    @Override
    protected List<Object> showTables() throws SQLException {
        return null;
    }

    @Override
    protected void switchDatabase(String databaseName) throws SQLException {

    }

    @Override
    protected Map<String, Object> queryMetaData(String tableName) throws SQLException {
        return null;
    }

    @Override
    protected String quote(String name) {
        return null;
    }

    public void setHadoopConfig(Map<String, Object> hadoopConfig){
        this.hadoopConfig = hadoopConfig;
    }
}

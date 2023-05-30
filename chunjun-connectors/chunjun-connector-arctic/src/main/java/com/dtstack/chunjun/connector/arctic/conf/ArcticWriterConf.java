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

package com.dtstack.chunjun.connector.arctic.conf;

import com.dtstack.chunjun.config.CommonConfig;

public class ArcticWriterConf extends CommonConfig {
    private String amsUrl;
    private String tableMode;
    private boolean isOverwrite;
    private String databaseName;
    private String tableName;
    public static final String KEYED_TABLE_MODE = "KEYED";
    public static final String UNKEYED_TABLE_MODE = "UNKEYED";

    public String getAmsUrl() {
        return amsUrl;
    }

    public void setAmsUrl(String amsUrl) {
        this.amsUrl = amsUrl;
    }

    public String getTableMode() {
        return tableMode;
    }

    public void setTableMode(String tableMode) {
        this.tableMode = tableMode;
    }

    public boolean isOverwrite() {
        return isOverwrite;
    }

    public void setOverwrite(boolean overwrite) {
        isOverwrite = overwrite;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return "ArcticWriterConf{" + "amsUrl='" + amsUrl + '\'' + '}';
    }
}

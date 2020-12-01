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
package com.dtstack.flinkx.rdb.outputformat;

import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormatBuilder;
import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Properties;

/**
 * @Company: www.dtstack.com
 * @author sishu.yss
 */
public class JdbcOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private JdbcOutputFormat format;

    public JdbcOutputFormatBuilder(JdbcOutputFormat format) {
        super.format = this.format = format;
    }

    public void setUsername(String username) {
        format.username = username;
    }

    public void setPassword(String password) {
        format.password = password;
    }

    public void setDriverName(String driverName) {
        format.driverName = driverName;
    }

    public void setDbUrl(String dbUrl) {
        format.dbUrl = dbUrl;
    }

    public void setPreSql(List<String> preSql) {
        format.preSql = preSql;
    }

    public void setPostSql(List<String> postSql) {
        format.postSql = postSql;
    }

    public void setUpdateKey(Map<String,List<String>> updateKey) {
        format.updateKey = updateKey;
    }

    public void setDatabaseInterface(DatabaseInterface databaseInterface) {
        format.databaseInterface = databaseInterface;
    }

    public void setProperties(Properties properties){
        format.properties = properties;
    }

    public void setMode(String mode) {
        format.mode = mode;
    }

    public void setTable(String table) {
        format.table = table;
    }

    public void setColumn(List<String> column) {
        format.column = column;
    }

    public void setFullColumn(List<String> fullColumn) {
        format.fullColumn = fullColumn;
    }

    public void setTypeConverter(TypeConverterInterface typeConverter ){
        format.typeConverter = typeConverter;
    }

    public void setInsertSqlMode(String insertSqlMode){
        format.insertSqlMode = insertSqlMode;
    }


    public void setSchema(String schema){
        format.setSchema(schema);
    }


    @Override
    protected void checkFormat() {
        if (format.username == null) {
            LOG.info("Username was not supplied separately.");
        }
        if (format.password == null) {
            LOG.info("Password was not supplied separately.");
        }
        if (format.dbUrl == null) {
            throw new IllegalArgumentException("No dababase URL supplied.");
        }
        if (format.driverName == null) {
            throw new IllegalArgumentException("No driver supplied");
        }

        if(format.getRestoreConfig().isRestore() && format.getBatchInterval() == 1){
            throw new IllegalArgumentException("Batch Size must greater than 1 when checkpoint is open");
        }
    }

}

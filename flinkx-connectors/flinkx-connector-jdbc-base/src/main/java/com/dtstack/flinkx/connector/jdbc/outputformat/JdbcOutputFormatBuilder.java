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
package com.dtstack.flinkx.connector.jdbc.outputformat;

import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormatBuilder;

import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.Properties;

/**
 * @author sishu.yss
 * @Company: www.dtstack.com
 */
public class JdbcOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private JdbcOutputFormat format;

    public JdbcOutputFormatBuilder() {
        super.format = this.format = new JdbcOutputFormat();
    }

    public JdbcOutputFormatBuilder setPreSql(List<String> preSql) {
        format.preSql = preSql;
        return this;
    }

    public JdbcOutputFormatBuilder setPostSql(List<String> postSql) {
        format.postSql = postSql;
        return this;
    }

    public JdbcOutputFormatBuilder setUpdateKey(String[] updateKey) {
        format.updateKey = updateKey;
        return this;
    }

//    public void setDatabaseInterface(DatabaseInterface databaseInterface) {
//        format.databaseInterface = databaseInterface;
//    }

    public JdbcOutputFormatBuilder setProperties(Properties properties) {
        format.properties = properties;
        return this;
    }

    public JdbcOutputFormatBuilder setMode(String mode) {
        format.mode = mode;
        return this;
    }

    public JdbcOutputFormatBuilder setJdbcOptions(JdbcOptions jdbcOptions) {
        format.jdbcOptions = jdbcOptions;
        return this;
    }

    public JdbcOutputFormatBuilder setRowType(RowType rowType) {
        format.rowType = rowType;
        return this;
    }

    public JdbcOutputFormatBuilder setColumn(String[] column) {
        format.column = column;
        return this;
    }

    public JdbcOutputFormatBuilder setFullColumn(String[] fullColumn) {
        format.fullColumn = fullColumn;
        return this;
    }

//    public void setTypeConverter(TypeConverterInterface typeConverter ){
//        format.typeConverter = typeConverter;
//    }

    public JdbcOutputFormatBuilder setInsertSqlMode(String insertSqlMode) {
        format.insertSqlMode = insertSqlMode;
        return this;
    }


    public JdbcOutputFormatBuilder setSchema(String schema) {
        format.setSchema(schema);
        return this;
    }

    public static JdbcOutputFormatBuilder builder() {
        return new JdbcOutputFormatBuilder();
    }

    @Override
    protected void checkFormat() {
        if (!format.jdbcOptions.getUsername().isPresent()) {
            LOG.info("Username was not supplied separately.");
        }
        if (!format.jdbcOptions.getPassword().isPresent()) {
            LOG.info("Password was not supplied separately.");
        }
        if (format.jdbcOptions.getDbURL() == null) {
            throw new IllegalArgumentException("No dababase URL supplied.");
        }
        if (format.jdbcOptions.getDriverName() == null) {
            throw new IllegalArgumentException("No driver supplied");
        }

//        if(format.getRestoreConfig().isRestore() && format.getBatchInterval() == 1){
//            throw new IllegalArgumentException("Batch Size must greater than 1 when checkpoint is open");
//        }
    }

    @Override
    public BaseRichOutputFormat finish() {
        format.jdbcRowConverter = format.jdbcOptions.getDialect().getRowConverter(format.rowType);
        return super.finish();
    }
}

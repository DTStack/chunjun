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

package com.dtstack.flinkx.rdb.inputformat;

import com.dtstack.flinkx.inputformat.RichInputFormatBuilder;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import com.dtstack.flinkx.reader.MetaColumn;

import java.util.List;

/**
 * The builder of JdbcInputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
@Deprecated
public class JdbcInputFormatBuilder extends RichInputFormatBuilder {

    private JdbcInputFormat format;

    public JdbcInputFormatBuilder() {
        super.format = format = new JdbcInputFormat();
    }

    public void setDrivername(String drivername) {
        format.drivername = drivername;
    }

    public void setDBUrl(String dbURL) {
        format.dbURL = dbURL;
    }

    public void setQuery(String query) {
        format.queryTemplate = query;
    }

    public void setParameterValues(Object[][] parameterValues) {
        format.parameterValues = parameterValues;
    }

    public void setUsername(String username) {
        format.username = username;
    }

    public void setPassword(String password) {
        format.password = password;
    }

    public void setTable(String table) {
        format.table = table;
    }

    public void setDatabaseInterface(DatabaseInterface databaseInterface) {
        format.databaseInterface = databaseInterface;
    }

    public void setTypeConverter(TypeConverterInterface converter){
        format.typeConverter = converter;
    }

    public void setMetaColumn(List<MetaColumn> metaColumns){
        format.metaColumns = metaColumns;
    }

    public void setFetchSize(int fetchSize){
        format.fetchSize = fetchSize;
    }

    public void setQueryTimeOut(int queryTimeOut){
        format.queryTimeOut = queryTimeOut;
    }

    public void setIncreCol(String increCol){
        format.increCol = increCol;
    }

    public void setStartLocation(String startLocation){
        format.startLocation = startLocation;
    }

    public void setIncreColType(String increColType){
        format.increColType = increColType;
    }

    @Override
    protected void checkFormat() {
        if (format.username == null) {
            LOG.info("Username was not supplied separately.");
        }
        if (format.password == null) {
            LOG.info("Password was not supplied separately.");
        }
        if (format.dbURL == null) {
            throw new IllegalArgumentException("No database URL supplied");
        }
        if (format.queryTemplate == null) {
            throw new IllegalArgumentException("No query supplied");
        }
        if (format.drivername == null) {
            throw new IllegalArgumentException("No driver supplied");
        }
    }

}

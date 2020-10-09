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

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.datareader.IncrementConfig;
import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * The builder of JdbcInputFormat
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class JdbcInputFormatBuilder extends BaseRichInputFormatBuilder {

    protected JdbcInputFormat format;

    public JdbcInputFormatBuilder(JdbcInputFormat format) {
        super.format = this.format = format;
    }

    public void setDriverName(String driverName) {
        format.driverName = driverName;
    }

    public void setDbUrl(String dbUrl) {
        format.dbUrl = dbUrl;
    }

    public void setQuery(String query) {
        format.queryTemplate = query;
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

    public void setSplitKey(String splitKey){
        format.splitKey = splitKey;
    }

    public void setNumPartitions(int numPartitions){
        format.numPartitions = numPartitions;
    }

    public void setCustomSql(String customSql){
        format.customSql = customSql;
    }

    public void setProperties(Properties properties){
        format.properties = properties;
    }

    public void setHadoopConfig(Map<String,Object> dirtyHadoopConfig) {
        format.hadoopConfig = dirtyHadoopConfig;
    }

    public void setIncrementConfig(IncrementConfig incrementConfig){
        format.incrementConfig = incrementConfig;
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
            throw new IllegalArgumentException("No database URL supplied");
        }

        if (format.driverName == null) {
            throw new IllegalArgumentException("No driver supplied");
        }

        if (StringUtils.isEmpty(format.splitKey) && format.numPartitions > 1){
            throw new IllegalArgumentException("Must specify the split column when the channel is greater than 1");
        }

        if (format.fetchSize > ConstantValue.MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("批量读取条数必须小于[200000]条");
        }
    }

}

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
import com.dtstack.flinkx.rdb.DataSource;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.loader.JdbcFormatLoader;
import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * The builder of DistributedJdbcInputFormat
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class DistributedJdbcInputFormatBuilder extends RichInputFormatBuilder {

    private static String DISTRIBUTED_TAG = "d";
    private DistributedJdbcInputFormat format;

    public DistributedJdbcInputFormatBuilder(String name) {
        JdbcFormatLoader jdbcFormatLoader = new JdbcFormatLoader(name + DISTRIBUTED_TAG, JdbcFormatLoader.INPUT_FORMAT);
        super.format = format = (DistributedJdbcInputFormat) jdbcFormatLoader.getFormatInstance();
    }

    public void setDrivername(String driverName) {
        format.driverName = driverName;
    }

    public void setUsername(String username) {
        format.username = username;
    }

    public void setPassword(String password) {
        format.password = password;
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

    public void setSplitKey(String splitKey){
        format.splitKey = splitKey;
    }

    public void setSourceList(List<DataSource> sourceList){
        format.sourceList = sourceList;
    }

    public void setNumPartitions(int numPartitions){
        format.numPartitions = numPartitions;
    }

    public void setWhere(String where){
        format.where = where;
    }

    public void setFetchSize(int fetchSize){
        format.fetchSize = fetchSize;
    }

    public void setQueryTimeOut(int queryTimeOut){
        format.queryTimeOut = queryTimeOut;
    }

    @Override
    protected void checkFormat() {

        boolean hasGlobalCountInfo = true;
        if(format.username == null || format.password == null){
            hasGlobalCountInfo = false;
        }

        if (format.sourceList == null || format.sourceList.size() == 0){
            throw new IllegalArgumentException("One or more data sources must be specified");
        }

        if (format.getRestoreConfig() != null && format.getRestoreConfig().isRestore()){
            throw new UnsupportedOperationException("This plugin not support restore from failed state");
        }

        String jdbcPrefix = null;

        for (DataSource dataSource : format.sourceList) {
            if(!hasGlobalCountInfo && (dataSource.getUserName() == null || dataSource.getPassword() == null)){
                throw new IllegalArgumentException("Must specify a global account or specify an account for each data source");
            }

            if (dataSource.getTable() == null || dataSource.getTable().length() == 0){
                throw new IllegalArgumentException("table name cannot be empty");
            }

            if (dataSource.getJdbcUrl() == null || dataSource.getJdbcUrl().length() == 0 ){
                throw new IllegalArgumentException("'jdbcUrl' cannot be empty");
            }

            if(jdbcPrefix == null){
                jdbcPrefix = dataSource.getJdbcUrl().split("//")[0];
            }

            if(!dataSource.getJdbcUrl().startsWith(jdbcPrefix)){
                throw new IllegalArgumentException("Multiple data sources must be of the same type");
            }

            if (StringUtils.isEmpty(format.splitKey) && format.numPartitions > 1){
                throw new IllegalArgumentException("Must specify the split column when the channel is greater than 1");
            }
        }
    }
}

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
package com.dtstack.flinkx.mysqld.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.mysql.MySqlDatabaseMeta;
import com.dtstack.flinkx.rdb.DataSource;
import com.dtstack.flinkx.rdb.datareader.DistributedJdbcDataReader;
import com.dtstack.flinkx.rdb.inputformat.DistributedJdbcInputFormat;
import com.dtstack.flinkx.rdb.inputformat.DistributedJdbcInputFormatBuilder;
import com.dtstack.flinkx.rdb.util.DbUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author toutian
 */
public class MysqldReader extends DistributedJdbcDataReader {

    public MysqldReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        setDatabaseInterface(new MySqlDatabaseMeta());
    }

    @Override
    protected DistributedJdbcInputFormatBuilder getBuilder(){
        return new DistributedJdbcInputFormatBuilder(new DistributedJdbcInputFormat());
    }

    @Override
    protected List<DataSource> buildConnections(){
        List<DataSource> sourceList = new ArrayList<>(connectionConfigs.size());
        for (ReaderConfig.ParameterConfig.ConnectionConfig connectionConfig : connectionConfigs) {
            String curUsername = (connectionConfig.getUsername() == null || connectionConfig.getUsername().length() == 0)
                    ? username : connectionConfig.getUsername();
            String curPassword = (connectionConfig.getPassword() == null || connectionConfig.getPassword().length() == 0)
                    ? password : connectionConfig.getPassword();
            String curJdbcUrl = DbUtil.formatJdbcUrl(connectionConfig.getJdbcUrl().get(0), Collections.singletonMap("zeroDateTimeBehavior", "convertToNull"));
            for (String table : connectionConfig.getTable()) {
                DataSource source = new DataSource();
                source.setTable(table);
                source.setUserName(curUsername);
                source.setPassword(curPassword);
                source.setJdbcUrl(curJdbcUrl);

                sourceList.add(source);
            }
        }

        return sourceList;
    }
}

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
package com.dtstack.flinkx.connector.mysql.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.dtstack.flinkx.RawTypeConverter;
import com.dtstack.flinkx.conf.FlinkXConf;
import com.dtstack.flinkx.connector.jdbc.inputFormat.JdbcInputFormatBuilder;
import com.dtstack.flinkx.connector.jdbc.source.JdbcDataSource;
import com.dtstack.flinkx.connector.mysql.MysqlDialect;
import com.dtstack.flinkx.connector.mysql.converter.MysqlTypeConverter;
import com.dtstack.flinkx.connector.mysql.inputFormat.MysqlInputFormat;
import org.apache.commons.lang3.StringUtils;

/**
 * Date: 2021/04/12
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class MysqlSource extends JdbcDataSource {

    public MysqlSource(FlinkXConf flinkXConf, StreamExecutionEnvironment env) {
        super(flinkXConf, env);
        super.jdbcDialect = new MysqlDialect();
        // 避免result.next阻塞
        if(jdbcConf.isPolling()
                && StringUtils.isEmpty(jdbcConf.getStartLocation())
                && jdbcConf.getFetchSize() == 0){
            jdbcConf.setFetchSize(1000);
        }
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return MysqlTypeConverter::apply;
    }

    @Override
    protected JdbcInputFormatBuilder getBuilder() {
        return new JdbcInputFormatBuilder(new MysqlInputFormat());
    }
}

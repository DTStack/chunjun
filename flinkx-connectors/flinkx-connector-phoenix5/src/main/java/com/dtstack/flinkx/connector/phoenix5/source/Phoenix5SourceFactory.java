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

package com.dtstack.flinkx.connector.phoenix5.source;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.flinkx.connector.jdbc.source.JdbcSourceFactory;
import com.dtstack.flinkx.connector.phoenix5.Phoenix5Dialect;
import com.dtstack.flinkx.connector.phoenix5.conf.Phoenix5Conf;
import com.dtstack.flinkx.connector.phoenix5.converter.Phoenix5RawTypeConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.StringUtils;

/**
 * @author wujuan
 * @version 1.0
 * @date 2021/7/9 16:01 星期五
 * @email wujuan@dtstack.com
 * @company www.dtstack.com
 */
public class Phoenix5SourceFactory extends JdbcSourceFactory {

    private Phoenix5Conf phoenix5Conf;

    public Phoenix5SourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env, new Phoenix5Dialect());
        phoenix5Conf = (Phoenix5Conf) jdbcConf;
        if (!phoenix5Conf.isReadFromHbase()) {
            if (jdbcConf.isPolling()
                    && StringUtils.isEmpty(jdbcConf.getStartLocation())
                    && jdbcConf.getFetchSize() == 0) {
                jdbcConf.setFetchSize(1000);
            }
        }
    }

    @Override
    protected Class<? extends JdbcConf> getConfClass() {
        return Phoenix5Conf.class;
    }

    @Override
    public DataStream<RowData> createSource() {
        if (phoenix5Conf.isReadFromHbase()) {
            HBaseInputFormatBuilder builder = new HBaseInputFormatBuilder();
            builder.setPhoenix5Conf(phoenix5Conf);
            // set sync task or sql task.
            phoenix5Conf.setSyncTaskType(useAbstractBaseColumn);
            return createInput(builder.finish());
        } else {
            return super.createSource();
        }
    }

    @Override
    protected JdbcInputFormatBuilder getBuilder() {
        return new Phoenix5InputFormatBuilder(new Phoenix5InputFormat());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return Phoenix5RawTypeConverter::apply;
    }
}

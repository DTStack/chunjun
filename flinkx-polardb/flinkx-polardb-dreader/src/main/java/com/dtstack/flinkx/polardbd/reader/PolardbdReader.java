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
package com.dtstack.flinkx.polardbd.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.mysqld.reader.MysqldReader;
import com.dtstack.flinkx.polardbd.PolardbDatabaseMeta;
import com.dtstack.flinkx.polardbd.format.PolardbdInputFormat;
import com.dtstack.flinkx.rdb.inputformat.DistributedJdbcInputFormat;
import com.dtstack.flinkx.rdb.inputformat.DistributedJdbcInputFormatBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Date: 2019/11/13
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class PolardbdReader extends MysqldReader {
    public PolardbdReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        setDatabaseInterface(new PolardbDatabaseMeta());
    }

    @Override
    protected DistributedJdbcInputFormatBuilder getBuilder(){
        return new DistributedJdbcInputFormatBuilder(new PolardbdInputFormat());
    }
}

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

package com.dtstack.flinkx;

import com.dtstack.flinkx.environment.StreamEnvConfigManager;
import com.dtstack.flinkx.exec.ExecuteProcessHelper;
import com.dtstack.flinkx.exec.ParamsInfo;
import com.dtstack.flinkx.parser.SqlParser;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chuixue
 * @create 2021-04-06 10:13
 * @description
 **/
public class SqlMain {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        ParamsInfo paramsInfo = ExecuteProcessHelper.parseParams(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FactoryUtil.setPluginPath(paramsInfo.getLocalSqlPluginPath());
        FactoryUtil.setEnv(env);
        // ds 原来的配置
        StreamEnvConfigManager.streamExecutionEnvironmentConfig(env, paramsInfo.getConfProp());
        StreamTableEnvironment tableEnv = StreamEnvConfigManager.getStreamTableEnv(env,paramsInfo);
        StatementSet statementSet = SqlParser.parseSql(paramsInfo, tableEnv);
        statementSet.execute();

        LOG.info("program {} execution success", paramsInfo.getName());
    }
}

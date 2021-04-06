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
import com.dtstack.flinkx.options.OptionParser;
import com.dtstack.flinkx.options.Options;
import com.dtstack.flinkx.parser.SqlParser;
import com.dtstack.flinkx.util.PluginUtil;
import com.google.common.base.Preconditions;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.dtstack.flinkx.exec.ExecuteProcessHelper.checkRemoteSqlPluginPath;
import static com.dtstack.flinkx.exec.ExecuteProcessHelper.getExternalJarUrls;

/**
 * @author chuixue
 * @create 2021-04-06 10:13
 * @description
 **/
public class SqlMain {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        LOG.info("------------program params-------------------------");
        Arrays.stream(args).forEach(arg -> LOG.info("{}", arg));
        LOG.info("-------------------------------------------");

        OptionParser optionParser = new OptionParser(args);
        Options options = optionParser.getOptions();

        String sql = URLDecoder.decode(options.getJob(), Charsets.UTF_8.name());
        String name = options.getJobName();
        String localSqlPluginPath = options.getPluginRoot();
        String remoteSqlPluginPath = options.getRemotePluginPath();
        String pluginLoadMode = options.getPluginLoadMode();
        String deployMode = options.getMode();
        String connectorLoadMode = options.getConnectorLoadMode();

        Preconditions.checkArgument(checkRemoteSqlPluginPath(remoteSqlPluginPath, deployMode, pluginLoadMode),
                "Non-local mode or shipfile deployment mode, remoteSqlPluginPath is required");
        String confProp = URLDecoder.decode(options.getConfProp(), Charsets.UTF_8.toString());
        Properties confProperties = PluginUtil.jsonStrToObject(confProp, Properties.class);

        List<URL> jarUrlList = getExternalJarUrls(options.getAddjar());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // ds 原来的配置
        StreamEnvConfigManager.streamExecutionEnvironmentConfig(env, confProperties);

        FactoryUtil.setPluginPath(StringUtils.isNotEmpty(localSqlPluginPath) ? localSqlPluginPath : remoteSqlPluginPath);
        FactoryUtil.setEnv(env);
        FactoryUtil.setConnectorLoadMode(connectorLoadMode);


        StreamTableEnvironment tableEnv = StreamEnvConfigManager.getStreamTableEnv(env, confProperties, name);
        StatementSet statementSet = SqlParser.parseSql(sql, jarUrlList, tableEnv);
        statementSet.execute();

        LOG.info("program {} execution success", name);
    }
}

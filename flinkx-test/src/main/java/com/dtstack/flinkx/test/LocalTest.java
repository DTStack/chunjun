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
package com.dtstack.flinkx.test;

import com.dtstack.flinkx.Main;
import com.dtstack.flinkx.util.GsonUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author jiangbo
 */
public class LocalTest {

    public static Logger LOG = LoggerFactory.getLogger(LocalTest.class);
    public static Configuration conf = new Configuration();

    public static void main(String[] args) throws Exception{
        Properties confProperties = new Properties();
//        String jobPath = "/Users/tudou/Library/Preferences/IntelliJIdea2019.3/scratches/json/stream/stream.json";
        String jobPath = "/Users/chuixue/Desktop/tmp/sqlFile.sql";
        // 任务配置参数
        List<String> argsList = new ArrayList<>();
        argsList.add("-mode");
        argsList.add("local");
        String content = readFile(jobPath);
        if(StringUtils.endsWith(jobPath, "json")){
            argsList.add("-job");
            argsList.add(content);
            argsList.add("-flinkconf");
            argsList.add(System.getProperty("user.dir") + "/flinkconf/");
//            argsList.add("-pluginRoot");
//            argsList.add("/Users/tudou/IdeaProjects/dt-center-flinkx/syncplugins");
            argsList.add("-confProp");
            argsList.add(GsonUtil.GSON.toJson(confProperties));
        }else if(StringUtils.endsWith(jobPath, "sql")){
            argsList.add("-connectorLoadMode");
            argsList.add("classloader");
            argsList.add("-job");
            argsList.add(URLEncoder.encode(content, StandardCharsets.UTF_8.name()));
            argsList.add("-jobName");
            argsList.add("flinkStreamSQLLocalTest");
            argsList.add("-pluginRoot");
            argsList.add("/Users/chuixue/dtstack/workspace/flinkx/syncplugins");
            argsList.add("-remotePluginPath");
            argsList.add("/Users/chuixue/dtstack/workspace/flinkx/syncplugins");
            argsList.add("-pluginLoadMode");
            argsList.add("LocalTest");
            argsList.add("-confProp");
//            argsList.add("{\"sql.env.parallelism\":\"2\",\"metrics.latency.interval\":\"30000\",\"metrics.latency.granularity\":\"operator\",\"time.characteristic\":\"eventTime\",\"state.backend\":\"FILESYSTEM\",\"state.checkpoints.dir\":\"hdfs://ns1/dtInsight/flink110/savepoints/POC_MEIDI_STREAM_JOIN\",\"sql.ttl.min\":\"5m\",\"sql.ttl.max\":\"10m\",\"flink.checkpoint.interval\":\"300000\",\"sql.checkpoint.mode\":\"EXACTLY_ONCE\",\"sql.checkpoint.timeout\":\"200000\",\"sql.max.concurrent.checkpoints\":\"1\",\"sql.checkpoint.cleanup.mode\":\"true\",\"timezone\":\"Asia/Shanghai\",\"early.trigger\":\"1\"}");
            argsList.add("{\"sql.env.parallelism\":\"2\",\"metrics.latency.interval\":\"30000\",\"metrics.latency.granularity\":\"operator\",\"time.characteristic\":\"eventTime\",\"sql.ttl.min\":\"5m\",\"sql.ttl.max\":\"10m\",\"flink.checkpoint.interval\":\"300000\",\"sql.checkpoint.mode\":\"EXACTLY_ONCE\",\"sql.checkpoint.timeout\":\"200000\",\"sql.max.concurrent.checkpoints\":\"1\",\"sql.checkpoint.cleanup.mode\":\"true\",\"timezone\":\"Asia/Shanghai\",\"early.trigger\":\"1\"}");
        }

        // Main.main(argsList.toArray(new String[0]));
        Main.main(argsList.toArray(new String[0]));

    }

    private static String readFile(String sqlPath) {
        try {
            byte[] array = Files.readAllBytes(Paths.get(sqlPath));
            return new String(array, StandardCharsets.UTF_8);
        } catch (IOException ioe) {
            LOG.error("Can not get the job info !!!", ioe);
            throw new RuntimeException(ioe);
        }
    }

}

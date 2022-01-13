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
package com.dtstack.flinkx.local.test;

import com.dtstack.flinkx.Main;
import com.dtstack.flinkx.util.GsonUtil;

import org.apache.commons.lang.StringUtils;
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

/** @author jiangbo */
public class LocalTest {

    public static Logger LOG = LoggerFactory.getLogger(LocalTest.class);

    public static void main(String[] args) throws Exception {
        LOG.warn("-----");
        Properties confProperties = new Properties();
//        confProperties.setProperty("flink.checkpoint.interval", "30000");
//        confProperties.setProperty("state.backend","ROCKSDB");
//        confProperties.setProperty("state.checkpoints.num-retained", "10");
//        confProperties.setProperty("state.checkpoints.dir", "file:///ck");
        String userDir = System.getProperty("user.dir");

        String jobPath =
                userDir + "/flinkx-examples/json/stream/stream.json";
        String flinkxDistDir = userDir + "/flinkx-dist";
        String s = "";

        // 任务配置参数
        List<String> argsList = new ArrayList<>();
        argsList.add("-mode");
        argsList.add("local");
        String content = readFile(jobPath);
        if (StringUtils.endsWith(jobPath, "json")) {
            argsList.add("-jobType");
            argsList.add("sync");
            argsList.add("-job");
            argsList.add(content);
//            argsList.add("-flinkConfDir");
//            argsList.add("/opt/dtstack/flink-1.12.2/conf/");
//            argsList.add("-confProp");
//            // 脏数据相关配置信息
//            StringBuilder stringBuilder = new StringBuilder();
//            stringBuilder
//                    .append("{")
//                    .append("\"flinkx.dirty-data.output-type\":\"print\"")
//                    .append(", ")
//                    .append("\"flinkx.dirty-data.max-rows\":\"1000\"")
//                    .append(", ")
//                    .append("\"flinkx.dirty-data.max-collect-failed-rows\":\"100\"")
//                    .append(", ")
//                    .append("\"flinkx.dirty-data.jdbc.url\":\"jdbc:mysql://localhost:3306/tiezhu\"")
//                    .append(", ")
//                    .append("\"flinkx.dirty-data.jdbc.username\":\"root\"")
//                    .append(", ")
//                    .append("\"flinkx.dirty-data.jdbc.password\":\"abc123\"")
//                    .append(", ")
//                    .append("\"flinkx.dirty-data.jdbc.database\":\"tiezhu\"")
//                    .append(", ")
//                    .append("\"flinkx.dirty-data.jdbc.table\":\"flinkx_dirty_data\"")
//                    .append(",")
//                    .append("\"flinkx.dirty-data.jdbc.batch-size\":\"10\"")
//                    .append(", ")
//                    .append("\"flinkx.dirty-data.log.print-interval\":\"10\"")
//                    .append("}");
//            argsList.add(stringBuilder.toString());
            argsList.add(GsonUtil.GSON.toJson(confProperties));
        } else if (StringUtils.endsWith(jobPath, "sql")) {
            argsList.add("-jobType");
            argsList.add("sql");
            argsList.add("-job");
            argsList.add(content);
//            argsList.add("-flinkConfDir");
//            argsList.add("/opt/dtstack/flink-1.12.2/conf/");
            argsList.add("-jobName");
            argsList.add("flinkStreamSQLLocalTest");
            argsList.add("-flinkxDistDir");
            argsList.add(flinkxDistDir);
            argsList.add("-remoteFlinkxDistDir");
            argsList.add(flinkxDistDir);
            argsList.add("-pluginLoadMode");
            argsList.add("LocalTest");
//            argsList.add("-confProp");
//            // 脏数据相关配置信息
//            StringBuilder stringBuilder = new StringBuilder();
//            stringBuilder
//                    .append("{")
//                    .append("\"flinkx.dirty-data.output-type\":\"mysql\"")
//                    .append(", ")
//                    .append("\"flinkx.dirty-data.max-rows\":\"1000\"")
//                    .append(", ")
//                    .append("\"flinkx.dirty-data.max-collect-failed-rows\":\"100\"")
//                    .append(", ")
//                    .append("\"flinkx.dirty-data.jdbc.url\":\"jdbc:mysql://localhost:3306/tiezhu\"")
//                    .append(", ")
//                    .append("\"flinkx.dirty-data.jdbc.username\":\"root\"")
//                    .append(", ")
//                    .append("\"flinkx.dirty-data.jdbc.password\":\"abc123\"")
//                    .append(", ")
//                    .append("\"flinkx.dirty-data.jdbc.database\":\"tiezhu\"")
//                    .append(", ")
//                    .append("\"flinkx.dirty-data.jdbc.table\":\"flinkx_dirty_data\"")
//                    .append(",")
//                    .append("\"flinkx.dirty-data.jdbc.batch-size\":\"10\"")
//                    .append(", ")
//                    .append("\"flinkx.dirty-data.log.print-interval\":\"10\"")
//                    .append("}");
//            argsList.add(stringBuilder.toString());
//            argsList.add("-confProp");
//            argsList.add("{\"execution.checkpointing.interval\":\"60000\"}");
//            argsList.add("{\"sql.checkpoint.mode\":\"AT_LEAST_ONCE\",\"flink.checkpoint.interval\":\"300000\"}");
//            argsList.add("{\"sql.checkpoint.mode\":\"EXACTLY_ONCE\",\"flink.checkpoint.interval\":\"300000\"}");
//            argsList.add("{\"sql.env.parallelism\":\"2\",\"metrics.latency.interval\":\"30000\",\"metrics.latency.granularity\":\"operator\",\"time.characteristic\":\"eventTime\",\"state.backend\":\"FILESYSTEM\",\"state.checkpoints.dir\":\"hdfs://ns1/dtInsight/flink110/savepoints/POC_MEIDI_STREAM_JOIN\",\"sql.ttl.min\":\"5m\",\"sql.ttl.max\":\"10m\",\"flink.checkpoint.interval\":\"300000\",\"sql.checkpoint.mode\":\"EXACTLY_ONCE\",\"sql.checkpoint.timeout\":\"200000\",\"sql.max.concurrent.checkpoints\":\"1\",\"sql.checkpoint.cleanup.mode\":\"true\",\"timezone\":\"Asia/Shanghai\",\"early.trigger\":\"1\"}");
//            argsList.add("{\"sql.env.parallelism\":\"2\",\"metrics.latency.interval\":\"30000\",\"metrics.latency.granularity\":\"operator\",\"time.characteristic\":\"eventTime\",\"sql.ttl.min\":\"5m\",\"sql.ttl.max\":\"10m\",\"flink.checkpoint.interval\":\"300000\",\"sql.checkpoint.mode\":\"EXACTLY_ONCE\",\"sql.checkpoint.timeout\":\"200000\",\"sql.max.concurrent.checkpoints\":\"1\",\"sql.checkpoint.cleanup.mode\":\"true\",\"timezone\":\"Asia/Shanghai\",\"early.trigger\":\"1\"}");
        }

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

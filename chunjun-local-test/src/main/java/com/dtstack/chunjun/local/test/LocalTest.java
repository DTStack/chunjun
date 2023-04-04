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
package com.dtstack.chunjun.local.test;

import com.dtstack.chunjun.Main;
import com.dtstack.chunjun.util.GsonUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Slf4j
public class LocalTest {

    public static void main(String[] args) throws Exception {
        Properties confProperties = new Properties();
        //        confProperties.setProperty("flink.checkpoint.interval", "30000");
        //        confProperties.setProperty("state.backend","ROCKSDB");
        //        confProperties.setProperty("state.checkpoints.num-retained", "10");
        //        confProperties.setProperty("state.checkpoints.dir", "file:///ck");
        String userDir = System.getProperty("user.dir");

        String jobPath =
                "/Users/wtz/job_place/chunjun/1.16/_02_SYNC/_05_Oceanbase/oceanbase_stream.json";
        String chunjunDistDir = userDir + "/chunjun-dist";
        String s = "";

        // 任务配置参数
        List<String> argsList = new ArrayList<>();
        argsList.add("-mode");
        argsList.add("localTest");
        // 替换脚本中的值
        // argsList.add("-p");
        // argsList.add("$aa=aaa, $bb=bbb");
        String content = readFile(jobPath);
        if (StringUtils.endsWith(jobPath, "json")) {
            argsList.add("-jobType");
            argsList.add("sync");
            argsList.add("-job");
            argsList.add(URLEncoder.encode(content, StandardCharsets.UTF_8.name()));
            // argsList.add("-flinkConfDir");
            // argsList.add("/opt/dtstack/flink-1.12.2/conf/");
            // argsList.add("-confProp");
            //// 脏数据相关配置信息
            // StringBuilder stringBuilder = new StringBuilder();
            // stringBuilder
            //        .append("{")
            //        .append("\"chunjun.dirty-data.output-type\":\"print\"")
            //        .append(", ")
            //        .append("\"chunjun.dirty-data.max-rows\":\"1000\"")
            //        .append(", ")
            //        .append("\"chunjun.dirty-data.max-collect-failed-rows\":\"100\"")
            //        .append(", ")
            //
            // .append("\"chunjun.dirty-data.jdbc.url\":\"jdbc:mysql://localhost:3306/tiezhu\"")
            //        .append(", ")
            //        .append("\"chunjun.dirty-data.jdbc.username\":\"root\"")
            //        .append(", ")
            //        .append("\"chunjun.dirty-data.jdbc.password\":\"abc123\"")
            //        .append(", ")
            //        .append("\"chunjun.dirty-data.jdbc.database\":\"tiezhu\"")
            //        .append(", ")
            //        .append("\"chunjun.dirty-data.jdbc.table\":\"chunjun_dirty_data\"")
            //        .append(",")
            //        .append("\"chunjun.dirty-data.jdbc.batch-size\":\"10\"")
            //        .append(", ")
            //        .append("\"chunjun.dirty-data.log.print-interval\":\"10\"")
            //        .append("}");
            // argsList.add(stringBuilder.toString());
            argsList.add(GsonUtil.GSON.toJson(confProperties));
        } else if (StringUtils.endsWith(jobPath, "sql")) {
            argsList.add("-jobType");
            argsList.add("sql");
            argsList.add("-job");
            argsList.add(URLEncoder.encode(content, StandardCharsets.UTF_8.name()));
            //            argsList.add("-flinkConfDir");
            //            argsList.add("/opt/dtstack/flink-1.12.2/conf/");
            argsList.add("-jobName");
            argsList.add("flinkStreamSQLLocalTest");
            // argsList.add("-chunjunDistDir");
            // argsList.add(chunjunDistDir);
            // argsList.add("-remoteChunJunDistDir");
            // argsList.add(chunjunDistDir);
            argsList.add("-pluginLoadMode");
            argsList.add("LocalTest");
            // argsList.add("-addjar");
            // argsList.add(GsonUtil.GSON.toJson(Collections.singleton("/opt/temp/aa.jar")));
            // argsList.add("-confProp");
            //// 脏数据相关配置信息
            // StringBuilder stringBuilder = new StringBuilder();
            // stringBuilder
            //        .append("{")
            //        .append("\"chunjun.dirty-data.output-type\":\"mysql\"")
            //        .append(", ")
            //        .append("\"chunjun.dirty-data.max-rows\":\"1000\"")
            //        .append(", ")
            //        .append("\"chunjun.dirty-data.max-collect-failed-rows\":\"100\"")
            //        .append(", ")
            //
            // .append("\"chunjun.dirty-data.jdbc.url\":\"jdbc:mysql://localhost:3306/tiezhu\"")
            //        .append(", ")
            //        .append("\"chunjun.dirty-data.jdbc.username\":\"root\"")
            //        .append(", ")
            //        .append("\"chunjun.dirty-data.jdbc.password\":\"abc123\"")
            //        .append(", ")
            //        .append("\"chunjun.dirty-data.jdbc.database\":\"tiezhu\"")
            //        .append(", ")
            //        .append("\"chunjun.dirty-data.jdbc.table\":\"chunjun_dirty_data\"")
            //        .append(",")
            //        .append("\"chunjun.dirty-data.jdbc.batch-size\":\"10\"")
            //        .append(", ")
            //        .append("\"chunjun.dirty-data.log.print-interval\":\"10\"")
            //        .append("}");
            // argsList.add(stringBuilder.toString());
            // argsList.add("-confProp");
            // argsList.add("{\"execution.checkpointing.interval\":\"60000\"}");

            // argsList.add("{\"sql.checkpoint.mode\":\"AT_LEAST_ONCE\",\"flink.checkpoint.interval\":\"300000\"}");
            //
            // argsList.add("{\"sql.checkpoint.mode\":\"EXACTLY_ONCE\",\"flink.checkpoint.interval\":\"300000\"}");
            //
            // argsList.add("{\"sql.env.parallelism\":\"2\",\"metrics.latency.interval\":\"30000\",\"metrics.latency.granularity\":\"operator\",\"time.characteristic\":\"eventTime\",\"state.backend\":\"FILESYSTEM\",\"state.checkpoints.dir\":\"hdfs://ns1/dtInsight/flink110/savepoints/POC_MEIDI_STREAM_JOIN\",\"sql.ttl.min\":\"5m\",\"sql.ttl.max\":\"10m\",\"flink.checkpoint.interval\":\"300000\",\"sql.checkpoint.mode\":\"EXACTLY_ONCE\",\"sql.checkpoint.timeout\":\"200000\",\"sql.max.concurrent.checkpoints\":\"1\",\"sql.checkpoint.cleanup.mode\":\"true\",\"timezone\":\"Asia/Shanghai\",\"early.trigger\":\"1\"}");
            //
            // argsList.add("{\"sql.env.parallelism\":\"2\",\"metrics.latency.interval\":\"30000\",\"metrics.latency.granularity\":\"operator\",\"time.characteristic\":\"eventTime\",\"sql.ttl.min\":\"5m\",\"sql.ttl.max\":\"10m\",\"flink.checkpoint.interval\":\"300000\",\"sql.checkpoint.mode\":\"EXACTLY_ONCE\",\"sql.checkpoint.timeout\":\"200000\",\"sql.max.concurrent.checkpoints\":\"1\",\"sql.checkpoint.cleanup.mode\":\"true\",\"timezone\":\"Asia/Shanghai\",\"early.trigger\":\"1\"}");

            /*
               python.files                             -pyfs
               python.client.executable                [flink configuration] -> [table api] -> [environment variable PYFLINK_CLIENT_EXECUTABLE]
               python.executable                       -pyexec
               python.requirements                     -pyreq
            */
            /* ---------------------------------------- pyFlink 测试 start --------------------------------------- */
            // Map<String, String> config = new HashMap<>();
            // config.put(
            //         "python.files",
            //
            // "/Users/lzq/Desktop/Projects/Flink/PyFlinkDemo/enjoyment.code/PythonUDFProvideToJava/test1.py");
            // config.put("python.client.executable", "python3");
            // config.put("python.executable", "python3");
            // config.put(
            //         "python.requirements",
            //
            // "/Users/lzq/Desktop/Projects/Flink/PyFlinkDemo/enjoyment.code/PythonUDFProvideToJava/requirements3.txt#/Users/lzq/Desktop/Projects/Flink/PyFlinkDemo/enjoyment.code/PythonUDFProvideToJava/cached_dir_binary3");
            // String configJsonString = GsonUtil.GSON.toJson(config);
            // argsList.add("-confProp");
            // argsList.add(configJsonString);
            /* ---------------------------------------- pyFlink 测试 end --------------------------------------- */
        }
        Main.main(argsList.toArray(new String[0]));
    }

    private static String readFile(String sqlPath) {
        try {
            byte[] array = Files.readAllBytes(Paths.get(sqlPath));
            return new String(array, StandardCharsets.UTF_8);
        } catch (IOException ioe) {
            log.error("Can not get the job info !!!", ioe);
            throw new RuntimeException(ioe);
        }
    }
}

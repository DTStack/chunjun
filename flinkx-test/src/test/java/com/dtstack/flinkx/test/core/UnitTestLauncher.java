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

package com.dtstack.flinkx.test.core;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.flinkx.constants.ConfigConstant;
import com.dtstack.flinkx.test.core.result.BaseResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

/**
 * @author jiangbo
 */
public abstract class UnitTestLauncher {

    public static Logger LOG = LoggerFactory.getLogger(UnitTestLauncher.class);

    private static final int FAILURE_RATE = 3;

    private static final int FAILURE_INTERVAL = 6;

    private static final int DELAY_INTERVAL = 10;

    protected String pluginName;

    protected String testCaseName;

    protected Configuration conf = new Configuration();

    protected String savepointPath;

    protected LinkedList<ParameterReplace> parameterReplaces = new LinkedList<>();

    protected int channel = 1;

    public abstract BaseResult runJob() throws Exception;

    public abstract String pluginType();

    public UnitTestLauncher withChannel(int channel) {
        this.channel = channel;
        return this;
    }

    public UnitTestLauncher withParameterReplace(ParameterReplace parameterReplace) {
        parameterReplaces.add(parameterReplace);
        return this;
    }

    public UnitTestLauncher withPluginName(String pluginName) {
        this.pluginName = pluginName;
        return this;
    }

    public UnitTestLauncher withTestCaseName(String testCaseName) {
        this.testCaseName = testCaseName;
        return this;
    }

    public UnitTestLauncher withConf(Configuration conf) {
        this.conf.addAll(conf);
        return this;
    }

    public UnitTestLauncher withSavepointPath(String savepointPath) {
        this.savepointPath = savepointPath;
        return this;
    }

    protected String readJob() {
        String userDir = System.getProperty("user.dir");
        String jobFilePath = String.format("%s/src/test/resources/testCase/%s/%s/%s.json", userDir, pluginName, pluginType(), testCaseName);
        return FileUtil.readFile(jobFilePath);
    }

    protected String replaceChannel(String job) {
        JSONObject jobJson = JSONObject.parseObject(job);
        jobJson.getJSONObject("job")
                .getJSONObject("setting")
                .getJSONObject("speed")
                .put("channel", channel);

        return jobJson.toJSONString();
    }

    protected void openCheckpointConf(StreamExecutionEnvironment env){
        if(conf == null){
            return;
        }

        if(!conf.containsKey(ConfigConstant.FLINK_CHECKPOINT_INTERVAL_KEY)){
            return;
        }else{
            long interval = conf.getLong(ConfigConstant.FLINK_CHECKPOINT_INTERVAL_KEY, -1);
            if (interval == -1) {
                return;
            }

            env.enableCheckpointing(interval);
            LOG.info("Open checkpoint with interval:" + interval);
        }

        String checkpointTimeoutStr = conf.getString(ConfigConstant.FLINK_CHECKPOINT_TIMEOUT_KEY, null);
        if(checkpointTimeoutStr != null){
            long checkpointTimeout = Long.parseLong(checkpointTimeoutStr);
            //checkpoints have to complete within one min,or are discard
            env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);

            LOG.info("Set checkpoint timeout:" + checkpointTimeout);
        }

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                FAILURE_RATE,
                Time.of(FAILURE_INTERVAL, TimeUnit.MINUTES),
                Time.of(DELAY_INTERVAL, TimeUnit.SECONDS)
        ));
    }
}

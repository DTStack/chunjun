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
package com.dtstack.flinkx.constants;


import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import static com.dtstack.flinkx.constants.ConstantValue.SHIP_FILE_PLUGIN_LOAD_MODE;

/**
 *
 * @author sishu.yss
 *
 */
public class ConfigConstant {

    public static final String FLINK_CHECKPOINT_TIMEOUT_KEY = "flink.checkpoint.timeout";

    public static final String KEY_PASSWORD = "password";
    public static final String KEY_CONFUSED_PASSWORD = "******";
    public static final String KEY_COLUMN = "column";
    public static final String KEY_BATCH_SIZE = "batchSize";

    //FlinkX Restart strategy
    public static final String STRATEGY_FIXED_DELAY = "fixedDelay";
    public static final String STRATEGY_FAILURE_RATE = "failureRate";
    public static final String STRATEGY_NO_RESTART = "NoRestart";

    public static final String STRATEGY_STRATEGY = "strategy";
    public static final String STRATEGY_RESTARTATTEMPTS = "restartAttempts";
    public static final String STRATEGY_DELAYINTERVAL = "delayInterval";
    public static final String STRATEGY_FAILURERATE = "failureRate";
    public static final String STRATEGY_FAILUREINTERVAL = "failureInterval";

    //FlinkX log pattern
    public static final String DEFAULT_LOG4J_PATTERN = "%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n";
    public static final String DEFAULT_LOGBACK_PATTERN = "%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{60} %X{sourceThread} - %msg%n";

    /**多少条打印一次日志*/
    public static final String SAMPLE_INTERVAL_COUNT = "sample.interval.count";

    /**重启策略*/
    public static final int FAILUEE_RATE = 3;

    /**窗口持续触发*/
    public static final String EARLY_TRIGGER = "early.trigger";

    /**全局并行度*/
    public static final String SQL_ENV_PARALLELISM = "sql.env.parallelism";

    /**最大并行度*/
    public static final String SQL_MAX_ENV_PARALLELISM = "sql.max.env.parallelism";

    /**缓冲超时时间*/
    public static final String SQL_BUFFER_TIMEOUT_MILLIS = "sql.buffer.timeout.millis";

    /**WATERMARK发送频率 default 200ms*/
    public static final String AUTO_WATERMARK_INTERVAL_KEY = "autoWatermarkInterval";

    /**是否从cp续跑*/
    public static final String RESTOREENABLE = "restore.enable";

    /**失败频率min*/
    public static final String FAILUREINTERVAL = "failure.interval";

    /**失败重启间隔时间 sec*/
    public static final String  DELAYINTERVAL= "delay.interval";

    /**时间语义*/
    public static final String FLINK_TIME_CHARACTERISTIC_KEY = "time.characteristic";

    /**cp时间间隔*/
    public static final String SQL_CHECKPOINT_INTERVAL_KEY = "sql.checkpoint.interval";
    public static final String FLINK_CHECKPOINT_INTERVAL_KEY = "flink.checkpoint.interval";

    /**cp是否对齐*/
    public static final String SQL_UNALIGNED_CHECKPOINTS = "sql.checkpoint.unalignedCheckpoints";

    /**cp模式*/
    public static final String SQL_CHECKPOINT_MODE_KEY = "sql.checkpoint.mode";
    public static final String FLINK_CHECKPOINT_MODE_KEY = "flink.checkpoint.mode";

    /**cp失败容忍的最大次数*/
    public static final String FLINK_CHECKPOINT_FAILURENUMBER_KEY = "flink.checkpoint.failurenumber";
    public static final String SQL_CHECKPOINT_FAILURENUMBER_KEY = "sql.checkpoint.failurenumber";

    /**同时支持cp个数*/
    public static final String FLINK_MAXCONCURRENTCHECKPOINTS_KEY = "sql.max.concurrent.checkpoints";

    /**cp清除模式*/
    public static final String SQL_CHECKPOINT_CLEANUPMODE_KEY = "sql.checkpoint.cleanup.mode";

    /**cp清除模式*/
    public static final String FLINK_CHECKPOINT_CLEANUPMODE_KEY = "flink.checkpoint.cleanup.mode";

    /**状态后端*/
    public static final String STATE_BACKEND_KEY = "state.backend";

    /**cp目录*/
    public static final String CHECKPOINTS_DIRECTORY_KEY = "state.checkpoints.dir";

    /**是否增量cp，rocksdb支持*/
    public static final String STATE_BACKEND_INCREMENTAL_KEY = "state.backend.incremental";

    /**状态存活最小时间*/
    public static final String SQL_TTL_MINTIME = "sql.ttl.min";

    /**状态存活最大时间*/
    public static final String SQL_TTL_MAXTIME = "sql.ttl.max";

    public static final String YARN_RESOURCE_MANAGER_WEBAPP_ADDRESS_KEY = "yarn.resourcemanager.webapp.address";

    public static final ConfigOption<String> FLINK_PLUGIN_LOAD_MODE_KEY = ConfigOptions
            .key("pluginLoadMode")
            .stringType()
            .defaultValue(SHIP_FILE_PLUGIN_LOAD_MODE)
            .withDescription("The config parameter defining YarnPer mode plugin loading method." +
                    "classpath: The plugin package is not uploaded when the task is submitted. " +
                    "The plugin package needs to be deployed in the pluginRoot directory of the yarn-node node, but the task starts faster" +
                    "shipfile: When submitting a task, upload the plugin package under the pluginRoot directory to deploy the plug-in package. " +
                    "The yarn-node node does not need to deploy the plugin package. " +
                    "The task startup speed depends on the size of the plugin package and the network environment.");
}

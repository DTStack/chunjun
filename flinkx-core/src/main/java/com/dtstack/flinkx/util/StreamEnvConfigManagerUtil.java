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

package com.dtstack.flinkx.util;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;

import com.dtstack.flinkx.constants.ConfigConstant;
import com.dtstack.flinkx.enums.EStateBackend;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.dtstack.flinkx.constants.ConfigConstant.STRATEGY_STRATEGY;

/**
 * 流执行环境相关配置
 * Date: 2019/11/22
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public final class StreamEnvConfigManagerUtil {
    /**
     * 设置TableEnvironment window提前触发
     *
     * @param tableEnv
     * @param confProperties
     */
    public static void streamTableEnvironmentEarlyTriggerConfig(TableEnvironment tableEnv, Properties confProperties) {
        confProperties = PropertiesUtil.propertiesTrim(confProperties);
        String triggerTime = confProperties.getProperty(ConfigConstant.EARLY_TRIGGER);
        if (StringUtils.isNumeric(triggerTime)) {
            TableConfig qConfig = tableEnv.getConfig();
            qConfig.getConfiguration().setString("table.exec.emit.early-fire.enabled", "true");
            qConfig.getConfiguration().setString("table.exec.emit.early-fire.delay", triggerTime + "s");
        }
    }

    /**
     * 设置任务执行的name
     *
     * @param tableEnv
     * @param name
     */
    public static void streamTableEnvironmentName(TableEnvironment tableEnv, String name) {
        tableEnv.getConfig().getConfiguration().setString(PipelineOptions.NAME, name);
    }

    /**
     * 设置TableEnvironment状态超时时间
     *
     * @param tableEnv
     * @param confProperties
     */
    public static void streamTableEnvironmentStateTTLConfig(TableEnvironment tableEnv, Properties confProperties) {
        confProperties = PropertiesUtil.propertiesTrim(confProperties);
        Optional<Tuple2<Time, Time>> tableEnvTTL = getTableEnvTTL(confProperties);
        if (tableEnvTTL.isPresent()) {
            Tuple2<Time, Time> timeRange = tableEnvTTL.get();
            TableConfig qConfig = tableEnv.getConfig();
            qConfig.setIdleStateRetentionTime(timeRange.f0, timeRange.f1);
        }
    }


    // -----------------------StreamExecutionEnvironment config-----------------------------------------------
    public static Optional<Integer> getEnvParallelism(Properties properties) {
        String parallelismStr = properties.getProperty(ConfigConstant.SQL_ENV_PARALLELISM);
        return StringUtils.isNotBlank(parallelismStr) ? Optional.of(Integer.valueOf(parallelismStr)) : Optional.empty();
    }

    public static Optional<Integer> getMaxEnvParallelism(Properties properties) {
        String parallelismStr = properties.getProperty(ConfigConstant.SQL_MAX_ENV_PARALLELISM);
        return StringUtils.isNotBlank(parallelismStr) ? Optional.of(Integer.valueOf(parallelismStr)) : Optional.empty();
    }

    public static Optional<Long> getBufferTimeoutMillis(Properties properties) {
        String mills = properties.getProperty(ConfigConstant.SQL_BUFFER_TIMEOUT_MILLIS);
        return StringUtils.isNotBlank(mills) ? Optional.of(Long.valueOf(mills)) : Optional.empty();
    }

    public static Optional<Long> getAutoWatermarkInterval(Properties properties) {
        String autoWatermarkInterval = properties.getProperty(ConfigConstant.AUTO_WATERMARK_INTERVAL_KEY);
        return StringUtils.isNotBlank(autoWatermarkInterval) ? Optional.of(Long.valueOf(autoWatermarkInterval)) : Optional.empty();
    }

    public static Optional<Boolean> isRestore(Properties properties) {
        String restoreEnable;
        if (properties.getProperty(STRATEGY_STRATEGY) != null) {
            restoreEnable = properties.getProperty(ConfigConstant.RESTOREENABLE, "false");
        } else {
            restoreEnable = properties.getProperty(ConfigConstant.RESTOREENABLE, "true");
        }
        return Optional.of(Boolean.valueOf(restoreEnable));
    }

    public static Optional<Integer> getDelayInterval(Properties properties) {
        String delayInterval = properties.getProperty(ConfigConstant.DELAYINTERVAL, "10");
        return Optional.of(Integer.valueOf(delayInterval));
    }

    public static Optional<Integer> getFailureInterval(Properties properties) {
        String failureInterval = properties.getProperty(ConfigConstant.FAILUREINTERVAL, "360");
        return Optional.of(Integer.valueOf(failureInterval));
    }

    /**
     * #ProcessingTime(默认), IngestionTime, EventTime
     *
     * @param properties
     */
    public static Optional<TimeCharacteristic> getStreamTimeCharacteristic(Properties properties) {
        if (!properties.containsKey(ConfigConstant.FLINK_TIME_CHARACTERISTIC_KEY)) {
            return Optional.empty();
        }
        String characteristicStr = properties.getProperty(ConfigConstant.FLINK_TIME_CHARACTERISTIC_KEY);
        Optional<TimeCharacteristic> characteristic = Arrays.stream(TimeCharacteristic.values())
                .filter(tc -> characteristicStr.equalsIgnoreCase(tc.toString())).findAny();

        if (!characteristic.isPresent()) {
            throw new RuntimeException("illegal property :" + ConfigConstant.FLINK_TIME_CHARACTERISTIC_KEY);
        }
        return characteristic;
    }

    public static Optional<Boolean> isCheckpointEnabled(Properties properties) {
        boolean checkpointEnabled = !(properties.getProperty(ConfigConstant.SQL_CHECKPOINT_INTERVAL_KEY) == null
                && properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_INTERVAL_KEY) == null);
        return Optional.of(checkpointEnabled);
    }

    public static Optional<Boolean> enableUnalignedCheckpoints(Properties properties) {
        String unalignedCheckpoints = properties.getProperty(ConfigConstant.SQL_UNALIGNED_CHECKPOINTS);
        if (!StringUtils.isEmpty(unalignedCheckpoints)) {
            return Optional.of(Boolean.valueOf(unalignedCheckpoints));
        }
        return Optional.empty();
    }

    /**
     * Task fails after cp reaches the number of failures
     * @param properties
     * @return
     */
    public static Optional<Integer> getTolerableCheckpointFailureNumber(Properties properties) {
        // 两个参数主要用来做上层兼容
        int sqlInterval = Integer.parseInt(properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_FAILURENUMBER_KEY, Integer.MAX_VALUE+""));
        int flinkInterval = Integer.parseInt(properties.getProperty(ConfigConstant.SQL_CHECKPOINT_FAILURENUMBER_KEY, Integer.MAX_VALUE+""));
        int checkpointInterval = Math.min(sqlInterval, flinkInterval);
        return Optional.of(checkpointInterval);
    }

    public static Optional<Long> getCheckpointInterval(Properties properties) {
        // 两个参数主要用来做上层兼容
        Long sqlInterval = Long.valueOf(properties.getProperty(ConfigConstant.SQL_CHECKPOINT_INTERVAL_KEY, "0"));
        Long flinkInterval = Long.valueOf(properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_INTERVAL_KEY, "0"));
        long checkpointInterval = Math.max(sqlInterval, flinkInterval);
        return Optional.of(checkpointInterval);
    }

    public static Optional<CheckpointingMode> getCheckpointMode(Properties properties) {
        String flinkCheckpointingModeStr = properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_MODE_KEY);
        String sqlCheckpointingModeStr = properties.getProperty(ConfigConstant.SQL_CHECKPOINT_MODE_KEY);
        CheckpointingMode checkpointingMode = null;
        if (!StringUtils.isEmpty(flinkCheckpointingModeStr)) {
            checkpointingMode = CheckpointingMode.valueOf(flinkCheckpointingModeStr.toUpperCase());
        }
        if (!StringUtils.isEmpty(sqlCheckpointingModeStr)) {
            checkpointingMode = CheckpointingMode.valueOf(sqlCheckpointingModeStr.toUpperCase());
        }
        return checkpointingMode == null ? Optional.of(CheckpointingMode.AT_LEAST_ONCE) : Optional.of(checkpointingMode);
    }

    public static Optional<Long> getCheckpointTimeout(Properties properties) {
        String checkpointTimeoutStr = properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_TIMEOUT_KEY);

        if (!StringUtils.isEmpty(checkpointTimeoutStr)) {
            Long checkpointTimeout = Long.valueOf(checkpointTimeoutStr);
            return Optional.of(checkpointTimeout);
        }
        return Optional.empty();
    }

    public static Optional<Integer> getMaxConcurrentCheckpoints(Properties properties) {
        String maxConcurrCheckpointsStr = properties.getProperty(ConfigConstant.FLINK_MAXCONCURRENTCHECKPOINTS_KEY);
        if (!StringUtils.isEmpty(maxConcurrCheckpointsStr)) {
            Integer maxConcurrCheckpoints = Integer.valueOf(maxConcurrCheckpointsStr);
            return Optional.of(maxConcurrCheckpoints);
        }
        return Optional.empty();
    }

    public static Optional<CheckpointConfig.ExternalizedCheckpointCleanup> getCheckpointCleanup(Properties properties) {
        Boolean sqlCleanMode = MathUtil.getBoolean(properties.getProperty(ConfigConstant.SQL_CHECKPOINT_CLEANUPMODE_KEY), false);
        Boolean flinkCleanMode = MathUtil.getBoolean(properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_CLEANUPMODE_KEY), false);

        CheckpointConfig.ExternalizedCheckpointCleanup externalizedCheckpointCleanup = (sqlCleanMode || flinkCleanMode) ?
                CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION : CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;
        return Optional.of(externalizedCheckpointCleanup);
    }

    public static Optional<StateBackend> getStateBackend(Properties properties) throws IOException {
        String backendType = properties.getProperty(ConfigConstant.STATE_BACKEND_KEY);
        String checkpointDataUri = properties.getProperty(ConfigConstant.CHECKPOINTS_DIRECTORY_KEY);
        String backendIncremental = properties.getProperty(ConfigConstant.STATE_BACKEND_INCREMENTAL_KEY, "true");

        if (!StringUtils.isEmpty(backendType)) {
            return createStateBackend(backendType, checkpointDataUri, backendIncremental);
        }
        return Optional.empty();
    }

    private static Optional<StateBackend> createStateBackend(String backendType, String checkpointDataUri, String backendIncremental) throws IOException {
        EStateBackend stateBackendType = EStateBackend.convertFromString(backendType);
        StateBackend stateBackend = null;
        switch (stateBackendType) {
            case MEMORY:
                stateBackend = new MemoryStateBackend();
                break;
            case FILESYSTEM:
                checkpointDataUriEmptyCheck(checkpointDataUri, backendType);
                stateBackend = new FsStateBackend(checkpointDataUri);
                break;
            case ROCKSDB:
                checkpointDataUriEmptyCheck(checkpointDataUri, backendType);
                stateBackend = new RocksDBStateBackend(checkpointDataUri, BooleanUtils.toBoolean(backendIncremental));
                break;
            default:
                break;
        }
        return stateBackend == null ? Optional.empty() : Optional.of(stateBackend);
    }

    private static void checkpointDataUriEmptyCheck(String checkpointDataUri, String backendType) {
        if (StringUtils.isEmpty(checkpointDataUri)) {
            throw new RuntimeException(backendType + " backend checkpointDataUri not null!");
        }
    }

    // -----------------TableEnvironment state ttl config------------------------------

    private static final String TTL_PATTERN_STR = "^+?([1-9][0-9]*)([dDhHmMsS])$";
    private static final Pattern TTL_PATTERN = Pattern.compile(TTL_PATTERN_STR);

    public static Optional<Tuple2<Time, Time>> getTableEnvTTL(Properties properties) {
        String ttlMintimeStr = properties.getProperty(ConfigConstant.SQL_TTL_MINTIME);
        String ttlMaxtimeStr = properties.getProperty(ConfigConstant.SQL_TTL_MAXTIME);
        if (StringUtils.isNotEmpty(ttlMintimeStr) || StringUtils.isNotEmpty(ttlMaxtimeStr)) {
            verityTtl(ttlMintimeStr, ttlMaxtimeStr);
            Matcher ttlMintimeStrMatcher = TTL_PATTERN.matcher(ttlMintimeStr);
            Matcher ttlMaxtimeStrMatcher = TTL_PATTERN.matcher(ttlMaxtimeStr);

            long ttlMintime = 0L;
            long ttlMaxtime = 0L;
            if (ttlMintimeStrMatcher.find()) {
                ttlMintime = getTtlTime(Integer.parseInt(ttlMintimeStrMatcher.group(1)), ttlMintimeStrMatcher.group(2));
            }
            if (ttlMaxtimeStrMatcher.find()) {
                ttlMaxtime = getTtlTime(Integer.parseInt(ttlMaxtimeStrMatcher.group(1)), ttlMaxtimeStrMatcher.group(2));
            }
            if (0L != ttlMintime && 0L != ttlMaxtime) {
                return Optional.of(new Tuple2<>(Time.milliseconds(ttlMintime), Time.milliseconds(ttlMaxtime)));
            }
        }
        return Optional.empty();
    }

    /**
     * ttl 校验
     *
     * @param ttlMintimeStr 最小时间
     * @param ttlMaxtimeStr 最大时间
     */
    private static void verityTtl(String ttlMintimeStr, String ttlMaxtimeStr) {
        if (null == ttlMintimeStr
                || null == ttlMaxtimeStr
                || !TTL_PATTERN.matcher(ttlMintimeStr).find()
                || !TTL_PATTERN.matcher(ttlMaxtimeStr).find()) {
            throw new RuntimeException("sql.ttl.min 、sql.ttl.max must be set at the same time . example sql.ttl.min=1h,sql.ttl.max=2h");
        }
    }

    /**
     * 不同单位时间到毫秒的转换
     *
     * @param timeNumber 时间值，如：30
     * @param timeUnit   单位，d:天，h:小时，m:分，s:秒
     * @return
     */
    private static Long getTtlTime(Integer timeNumber, String timeUnit) {
        if ("d".equalsIgnoreCase(timeUnit)) {
            return timeNumber * 1000L * 60 * 60 * 24;
        } else if ("h".equalsIgnoreCase(timeUnit)) {
            return timeNumber * 1000L * 60 * 60;
        } else if ("m".equalsIgnoreCase(timeUnit)) {
            return timeNumber * 1000L * 60;
        } else if ("s".equalsIgnoreCase(timeUnit)) {
            return timeNumber * 1000L;
        } else {
            throw new RuntimeException("not support " + timeNumber + timeUnit);
        }
    }

    public static void disableChainOperator(StreamExecutionEnvironment env, Configuration configuration) {
        if (configuration.getBoolean("disableChainOperator", false)) {
            env.disableOperatorChaining();
        }
    }
}

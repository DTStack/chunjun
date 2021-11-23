package com.dtstack.flinkx.connector.dorisbatch.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.List;

/**
 * Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2021-11-21
 */
public class DorisOptions {
    public static final ConfigOption<List<String>> FENODES =
            ConfigOptions.key("feNodes")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("YOUR DORIS FE HOSTNAME AND RESFUL PORT");

    public static final ConfigOption<String> TABLE_IDENTIFY =
            ConfigOptions.key("table.identifier")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("YOUR DORIS DATABASE NAME AND YOUR DORIS TABLE NAME");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("YOUR DORIS CONNECTOR USER NAME");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("YOUR DORIS CONNECTOR  PASSWORD");

    public static final ConfigOption<Integer> REQUEST_TABLET_SIZE =
            ConfigOptions.key("requestTabletSize")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription("");

    public static final ConfigOption<Integer> REQUEST_CONNECT_TIMEOUT_MS =
            ConfigOptions.key("requestConnectTimeoutMs")
                    .intType()
                    .defaultValue(DorisKeys.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<Integer> REQUEST_READ_TIMEOUT_MS =
            ConfigOptions.key("requestReadTimeoutMs")
                    .intType()
                    .defaultValue(DorisKeys.DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<Integer> REQUEST_QUERY_TIMEOUT_SEC =
            ConfigOptions.key("requestQueryTimeoutS")
                    .intType()
                    .defaultValue(DorisKeys.DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<Integer> REQUEST_RETRIES =
            ConfigOptions.key("requestRetries")
                    .intType()
                    .defaultValue(DorisKeys.DORIS_REQUEST_RETRIES_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<Integer> REQUEST_BATCH_SIZE =
            ConfigOptions.key("requestBatchSize")
                    .intType()
                    .defaultValue(DorisKeys.DORIS_BATCH_SIZE_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<Long> EXEC_MEM_LIMIT =
            ConfigOptions.key("requestBatchSize")
                    .longType()
                    .defaultValue(DorisKeys.DORIS_EXEC_MEM_LIMIT_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<Integer> DESERIALIZE_QUEUE_SIZE =
            ConfigOptions.key("deserializeQueueSize")
                    .intType()
                    .defaultValue(DorisKeys.DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<Boolean> DESERIALIZE_ARROW_ASYNC =
            ConfigOptions.key("deserializeArrowAsync")
                    .booleanType()
                    .defaultValue(DorisKeys.DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<String> FIELD_DELIMITER =
            ConfigOptions.key("fieldDelimiter")
                    .stringType()
                    .defaultValue(DorisKeys.FIELD_DELIMITER)
                    .withDescription("");

    public static final ConfigOption<String> LINE_DELIMITER =
            ConfigOptions.key("lineDelimiter")
                    .stringType()
                    .defaultValue(DorisKeys.LINE_DELIMITER)
                    .withDescription("");

    public static final ConfigOption<Integer> MAX_RETRIES =
            ConfigOptions.key("maxRetries")
                    .intType()
                    .defaultValue(DorisKeys.DORIS_MAX_RETRIES_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<String> WRITE_MODE =
            ConfigOptions.key("writeMode")
                    .stringType()
                    .defaultValue(DorisKeys.DORIS_WRITE_MODE_DEFAULT)
                    .withDescription("");

    public static final ConfigOption<Integer> BATCH_SIZE =
            ConfigOptions.key("batchSize")
                    .intType()
                    .defaultValue(DorisKeys.DORIS_BATCH_SIZE_DEFAULT)
                    .withDescription("");
}

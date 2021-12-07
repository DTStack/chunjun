package com.dtstack.flinkx.pulsar.format;

/**
 * The Constant of PulsarReader
 * <p>
 * Company: www.dtstack.com
 *
 * @author fengjiangtao_yewu@cmss.chinamobile.com 2021-3-23
 */
public class Constants {
    public static final String KEY_TOPIC = "topic";
    public static final String KEY_PULSAR_SERVICE_URL = "pulsarServiceUrl";
    public static final String KEY_CONSUMER_SETTINGS = "consumerSettings";
    public static final String KEY_TOKEN = "token";
    public static final String KEY_BLANK_IGNORE = "blankIgnore";
    public static final String KEY_CODEC = "codec";
    public static final String KEY_TIMEOUT = "timeout";
    public static final String KEY_INITIAL_POSITION = "initialPosition";
    public static final String KEY_FIELD_DELIMITER = "fieldDelimiter";
    public static final String KEY_LISTENER_NAME = "listenerName";
    public static final String KEY_BATCH_INTERVAL = "batchInterval";
    public static final String KEY_BATCH_BYTES = "batchBytes";
    public static final String KEY_BATCH_TIME = "batchTime";
    public static final int DEFAULT_BATCH_INTERVAL = 1;
    // 默认10MB
    public static final int DEFAULT_BATCH_BYTES = 10 * 1024 * 1024;
    // 默认100ms
    public static final int DEFAULT_BATCH_TIME = 100;

    public static final String DEFAULT_FIELD_DELIMITER = ",";
    public static final int DEFAULT_TIMEOUT = 10000;

    public static final String CONSUMER_SUBSCRIPTION_NAME = "flinkx-pulsarreader";
}

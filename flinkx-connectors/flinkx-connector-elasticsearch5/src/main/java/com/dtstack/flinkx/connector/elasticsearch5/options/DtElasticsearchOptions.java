package com.dtstack.flinkx.connector.elasticsearch5.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/06/28 19:29
 */
public class DtElasticsearchOptions {

    public static final ConfigOption<String> CLUSTER_OPTION = ConfigOptions
            .key("cluster").stringType().defaultValue("elasticsearch").withDescription("Elasticsearch cluster name to connect to.");

    public static final ConfigOption<Integer> ACTION_TIMEOUT_OPTION = ConfigOptions
            .key("action-timeout").intType().defaultValue(5000).withDescription("timeout when interaction with es.");

}

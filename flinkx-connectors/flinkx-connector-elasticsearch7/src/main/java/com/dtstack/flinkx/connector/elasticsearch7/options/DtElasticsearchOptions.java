package com.dtstack.flinkx.connector.elasticsearch7.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/06/29 19:10
 */
public class DtElasticsearchOptions {

    public static final ConfigOption<Integer> DT_BULK_FLUSH_MAX_ACTIONS_OPTION =
            ConfigOptions.key("bulk-flush.max-actions")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Maximum number of actions to buffer for each bulk request.");

    public static final ConfigOption<Integer> DT_PARALLELISM_OPTION =
            ConfigOptions.key("parallelism")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Parallelism for connector running.");

    public static final ConfigOption<Integer> DT_CLIENT_CONNECT_TIMEOUT_OPTION =
            ConfigOptions.key("client.connect-timeout")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Elasticsearch client max connect timeout. default: 1000 ms");

    public static final ConfigOption<Integer> DT_CLIENT_SOCKET_TIMEOUT_OPTION =
            ConfigOptions.key("client.socket-timeout")
                    .intType()
                    .defaultValue(1800000)
                    .withDescription("Elasticsearch client max socket timeout. default: 1800000 ms.");

    public static final ConfigOption<Integer> DT_CLIENT_KEEPALIVE_TIME_OPTION =
            ConfigOptions.key("client.keep-alive-time")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Elasticsearch client connection max keepAlive time. default: 1000 ms");

    public static final ConfigOption<Integer> DT_CLIENT_REQUEST_TIMEOUT_OPTION =
            ConfigOptions.key("client.request-timeout")
                    .intType()
                    .defaultValue(2000)
                    .withDescription("Elasticsearch client connection max request timeout. default:2000 ms");

    public static final ConfigOption<Integer> DT_CLIENT_MAX_CONNECTION_PER_ROUTE_OPTION =
            ConfigOptions.key("client.max-connection-per-route")
                    .intType()
                    .defaultValue(10)
                    .withDescription("Elasticsearch client connection assigns maximum connection per route value. default:10 ");

}

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

}

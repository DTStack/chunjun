package com.dtstack.flinkx.connector.es.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/06/29 21:40
 */
public class DtEsOptions {

    public static final ConfigOption<Integer> DT_BULK_FLUSH_MAX_ACTIONS_OPTION =
            ConfigOptions.key("bulk-flush.max-actions")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Maximum number of actions to buffer for each bulk request.");

}

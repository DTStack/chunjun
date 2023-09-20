package com.dtstack.chunjun.config;

import org.apache.flink.configuration.ConfigOption;

import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2023-09-13
 */
public class ChunJunServerOptions {

    public static final ConfigOption<String> CHUNJUN_DIST_PATH =
            key("chunjun.dist.path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "config the path of chunjun root dir.");
}

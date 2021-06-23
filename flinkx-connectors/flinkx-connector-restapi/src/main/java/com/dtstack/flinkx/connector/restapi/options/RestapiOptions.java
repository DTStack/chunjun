package com.dtstack.flinkx.connector.restapi.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class RestapiOptions {

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("api url.");

    public static final ConfigOption<String> DECODE =
            ConfigOptions.key("decode")
                    .stringType()
                    .defaultValue("json")
                    .withDescription("decode type");

    public static final ConfigOption<Long> INTERVALTIME =
            ConfigOptions.key("intervalTime")
                    .longType()
                    .defaultValue(3000L)
                    .withDescription("");

    public static final ConfigOption<String> REQUESTMODE =
            ConfigOptions.key("requestMode")
                    .stringType()
                    .defaultValue("post")
                    .withDescription("");

    public static final ConfigOption<String> COLUMN =
            ConfigOptions.key("column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("");

    public static final ConfigOption<String> HEADER =
            ConfigOptions.key("header")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("");

    public static final ConfigOption<String> BODY =
            ConfigOptions.key("body")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("");


    public static final ConfigOption<String> PARAMS =
            ConfigOptions.key("params")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("");

}

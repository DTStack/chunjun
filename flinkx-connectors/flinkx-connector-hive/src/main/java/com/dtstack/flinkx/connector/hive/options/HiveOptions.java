/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.connector.hive.options;

import com.dtstack.flinkx.connector.hdfs.options.HdfsOptions;
import com.dtstack.flinkx.connector.hive.enums.PartitionEnum;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Date: 2021/06/24 Company: www.dtstack.com
 *
 * @author tudou
 */
public class HiveOptions extends HdfsOptions {
    public static final ConfigOption<String> JDBC_URL =
            ConfigOptions.key("url").stringType().noDefaultValue().withDescription("hive jdbc url");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("hive jdbc username");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("hive jdbc password");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("hive tableName");

    public static final ConfigOption<String> PARTITION_TYPE =
            ConfigOptions.key("partition-type")
                    .stringType()
                    .defaultValue(PartitionEnum.DAY.name())
                    .withDescription(
                            "Partition types, including DAY, HOUR, and MINUTE. If the partition does not exist, it will be created automatically. The time of the automatically created partition is based on the server time of the current task.");

    public static final ConfigOption<String> PARTITION =
            ConfigOptions.key("partition")
                    .stringType()
                    .defaultValue("pt")
                    .withDescription("Partition field name");
}

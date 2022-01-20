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
package com.dtstack.flinkx.connector.inceptor.options;

import com.dtstack.flinkx.connector.inceptor.enums.PartitionEnum;
import com.dtstack.flinkx.connector.jdbc.options.JdbcCommonOptions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.HashMap;
import java.util.Map;

import static com.dtstack.flinkx.security.KerberosUtil.KRB5_CONF_KEY;

/**
 * Date: 2021/06/24 Company: www.dtstack.com
 *
 * @author tudou
 */
public class InceptorOptions extends JdbcCommonOptions {

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

    public static Map<String, Object> getKerberosConfig(Map<String, String> tableOptions) {
        Map<String, Object> hadoopConfig = new HashMap<>();
        if (hasHadoopConfig(tableOptions)) {
            tableOptions.keySet().stream()
                    .filter((key) -> key.startsWith("security.kerberos"))
                    .forEach(
                            (key) -> {
                                String value = tableOptions.get(key);
                                String subKey = key.substring("security.kerberos.".length());
                                if ("keytab".equals(subKey)) {
                                    hadoopConfig.put("principalFile", value);
                                } else if ("krb5.conf".equals(subKey)) {
                                    hadoopConfig.put(KRB5_CONF_KEY, value);
                                }
                                hadoopConfig.put(subKey, value);
                            });
        }

        return hadoopConfig;
    }

    private static boolean hasHadoopConfig(Map<String, String> tableOptions) {
        return tableOptions.keySet().stream().anyMatch((k) -> k.startsWith("security.kerberos."));
    }
}

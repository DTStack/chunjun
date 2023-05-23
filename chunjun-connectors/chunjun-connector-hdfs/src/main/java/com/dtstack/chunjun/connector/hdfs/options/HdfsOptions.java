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
package com.dtstack.chunjun.connector.hdfs.options;

import com.dtstack.chunjun.table.options.BaseFileOptions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.HashMap;
import java.util.Map;

public class HdfsOptions extends BaseFileOptions {
    public static final ConfigOption<String> DEFAULT_FS =
            ConfigOptions.key("default-fs")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hadoop hdfs file system nameNode node address");

    public static final ConfigOption<String> FILE_TYPE =
            ConfigOptions.key("file-type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "File type, currently only supports user configuration as text, orc, parquet");

    public static final ConfigOption<String> FILTER_REGEX =
            ConfigOptions.key("filter-regex")
                    .stringType()
                    .defaultValue("")
                    .withDescription("File regular expression, read the matched file");

    public static final ConfigOption<String> FIELD_DELIMITER =
            ConfigOptions.key("field-delimiter")
                    .stringType()
                    .defaultValue("\001")
                    .withDescription("The separator of the field when fileType is text");

    public static final ConfigOption<Boolean> ENABLE_DICTIONARY =
            ConfigOptions.key("enable-dictionary")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("enable dictionary");

    public static Map<String, Object> getHadoopConfig(Map<String, String> tableOptions) {
        Map<String, Object> hadoopConfig = new HashMap<>();
        if (hasHadoopConfig(tableOptions)) {
            tableOptions.keySet().stream()
                    .filter((key) -> key.startsWith("properties."))
                    .forEach(
                            (key) -> {
                                String value = tableOptions.get(key);
                                String subKey = key.substring("properties.".length());
                                hadoopConfig.put(subKey, value);
                            });
        }

        return hadoopConfig;
    }

    private static boolean hasHadoopConfig(Map<String, String> tableOptions) {
        return tableOptions.keySet().stream().anyMatch((k) -> k.startsWith("properties."));
    }

    public static final ConfigOption<String> SINK_COMMIT_FINISHED_FILE_NAME =
            ConfigOptions.key("sink.commit.finished-file.name")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The file name for finished-file partition commit policy");
}

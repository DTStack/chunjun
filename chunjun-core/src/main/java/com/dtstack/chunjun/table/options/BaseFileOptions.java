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
package com.dtstack.chunjun.table.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class BaseFileOptions {
    public static final ConfigOption<String> PATH =
            ConfigOptions.key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The path of the data file");

    public static final ConfigOption<String> FILE_NAME =
            ConfigOptions.key("file-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Data file directory name");

    public static final ConfigOption<String> WRITE_MODE =
            ConfigOptions.key("write-mode")
                    .stringType()
                    .defaultValue("append")
                    .withDescription("Data cleaning processing mode before hdfs writer write:");

    public static final ConfigOption<String> COMPRESS =
            ConfigOptions.key("compress")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("hdfs file compression type");

    public static final ConfigOption<String> ENCODING =
            ConfigOptions.key("encoding")
                    .stringType()
                    .defaultValue("UTF-8")
                    .withDescription("Encoding format can be configured when fileType is text");

    public static final ConfigOption<Long> MAX_FILE_SIZE =
            ConfigOptions.key("max-file-size")
                    .longType()
                    .defaultValue(1073741824L)
                    .withDescription("The maximum size of a single file written to hdfs, in bytes");

    public static final ConfigOption<Long> NEXT_CHECK_ROWS =
            ConfigOptions.key("next-check-rows")
                    .longType()
                    .defaultValue(5000L)
                    .withDescription("The number of data written in the next file size check");

    public static final ConfigOption<String> JOB_IDENTIFIER =
            ConfigOptions.key("properties.job-identifier")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "To solve the problem of file loss caused by multiple tasks writing to the same output source at the same time");
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.ftp.options;

import com.dtstack.chunjun.table.options.BaseFileOptions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class FtpOptions extends BaseFileOptions {

    public static final ConfigOption<String> FORMAT =
            ConfigOptions.key("format")
                    .stringType()
                    .defaultValue("csv")
                    .withDescription(
                            "Defines the format identifier for encoding value data. "
                                    + "The identifier is used to discover a suitable format factory.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ftp username");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ftp password");

    public static final ConfigOption<String> PROTOCOL =
            ConfigOptions.key("protocol")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("ftp protocol");

    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host").stringType().noDefaultValue().withDescription("ftp host");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "When protocol is FTP, the default port is 21; When the protocol is SFTP, the port defaults to 22");

    public static final ConfigOption<Integer> TIMEOUT =
            ConfigOptions.key("timeout")
                    .intType()
                    .defaultValue(5000)
                    .withDescription("ftp timeout");

    public static final ConfigOption<String> CONNECT_PATTERN =
            ConfigOptions.key("connect-pattern")
                    .stringType()
                    .defaultValue("PASV")
                    .withDescription("ftp connectPattern");

    public static final ConfigOption<Boolean> FIRST_LINE_HEADER =
            ConfigOptions.key("first-line-header")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("skip header");

    public static final ConfigOption<String> FILE_TYPE =
            ConfigOptions.key("file-type")
                    .stringType()
                    .defaultValue("TXT")
                    .withDescription("file type");

    public static final ConfigOption<String> FIELD_DELIMITER =
            ConfigOptions.key("field-delimiter")
                    .stringType()
                    .defaultValue(",")
                    .withDescription("field delimiter");

    public static final ConfigOption<String> COMPRESS_TYPE =
            ConfigOptions.key("compress-type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("compress type");
    public static final ConfigOption<String> SHEET_NO =
            ConfigOptions.key("sheet-no")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("sheet no, Multiple numbers separated by commas(,)");
    public static final ConfigOption<String> COLUMN_INDEX =
            ConfigOptions.key("column-index")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("column index, Multiple numbers separated by commas(,)");
}

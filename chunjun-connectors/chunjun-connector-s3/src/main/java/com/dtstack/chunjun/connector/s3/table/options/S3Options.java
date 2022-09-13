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

package com.dtstack.chunjun.connector.s3.table.options;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

public class S3Options {
    public static final ConfigOption<String> ACCESS_Key =
            key("assessKey").stringType().noDefaultValue().withDescription("aws_access_key_id");

    public static final ConfigOption<String> SECRET_Key =
            key("secretKey").stringType().noDefaultValue().withDescription("aws_secret_access_key");

    public static final ConfigOption<String> BUCKET =
            key("bucket").stringType().noDefaultValue().withDescription("aws_bucket_name");

    public static final ConfigOption<String> OBJECTS =
            key("objects")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("object to be synchronized. supports regular expressions");

    public static final ConfigOption<String> OBJECT =
            key("object")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("writer file object, can only be one");

    public static final ConfigOption<String> FIELD_DELIMITER =
            key("fieldDelimiter")
                    .stringType()
                    .defaultValue(",")
                    .withDescription("the field delimiter to read");

    public static final ConfigOption<String> ENCODING =
            key("encoding")
                    .stringType()
                    .defaultValue("UTF-8")
                    .withDescription("read the encoding configuration of the file");

    public static final ConfigOption<String> REGION =
            key("region")
                    .stringType()
                    .defaultValue("us-west-2")
                    .withDescription("an area where buckets are stored");

    public static final ConfigOption<Boolean> IS_FIRST_LINE_HEADER =
            key("isFirstLineHeader")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "whether the first line is a header line, if so, the first line is not read");
}

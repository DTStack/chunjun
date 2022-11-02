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

package com.dtstack.chunjun.connector.doris.options;

public final class DorisKeys {

    public static final String FIELD_DELIMITER_KEY = "fieldDelimiter";

    public static final String LINE_DELIMITER_KEY = "lineDelimiter";

    public static final String FILTER_QUERY_KEY = "filterQuery";

    public static final String BATCH_SIZE_KEY = "batchSize";

    public static final String BATCH_INTERNAL_MS_KEY = "batchIntervalMs";

    public static final String FLUSH_INTERNAL_MS_KEY = "flushIntervalMills";

    public static final String WAITRETRIES_MS_KEY = "waitRetryMills";

    public static final String MAX_RETRIES_KEY = "maxRetries";

    public static final String DATABASE_KEY = "database";

    public static final String TABLE_KEY = "table";

    public static final String LOAD_OPTIONS_KEY = "loadOptions";

    public static final String LOAD_PROPERTIES_KEY = "loadProperties";

    public static final String REQUEST_TABLET_SIZE_KEY = "requestTabletSize";

    public static final String REQUEST_CONNECT_TIMEOUT_MS_KEY = "requestConnectTimeoutMs";

    public static final String REQUEST_READ_TIMEOUT_MS_KEY = "requestReadTimeoutMs";

    public static final String REQUEST_QUERY_TIMEOUT_S_KEY = "requestQueryTimeoutS";

    public static final String REQUEST_RETRIES_KEY = "requestRetries";

    public static final String REQUEST_BATCH_SIZE_KEY = "requestBatchSize";

    public static final String EXEC_MEM_LIMIT_KEY = "execMemLimit";

    public static final String DESERIALIZE_QUEUE_SIZE_KEY = "deserializeQueueSize";

    public static final String DESERIALIZE_ARROW_ASYNC_KEY = "deserializeArrowAsync";

    public static final String FE_NODES_KEY = "feNodes";

    public static final String USER_NAME_KEY = "username";

    public static final String PASSWORD_KEY = "password";

    public static final String WRITE_MODE_KEY = "writeMode";

    public static final String FIELD_DELIMITER = "\t";

    public static final String LINE_DELIMITER = "\n";

    public static final String DORIS_DEFAULT_CLUSTER = "default_cluster";

    public static final Integer DORIS_REQUEST_RETRIES_DEFAULT = 3;

    public static final Integer DORIS_MAX_RETRIES_DEFAULT = 3;

    public static final String DORIS_WRITE_MODE_DEFAULT = "APPEND";

    public static final Integer DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT = 30 * 1000;

    public static final Integer DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT = 30 * 1000;

    public static final Integer DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT = 3600;

    public static final String DORIS_TABLET_SIZE = "doris.request.tablet.size";

    public static final Integer DORIS_TABLET_SIZE_DEFAULT = Integer.MAX_VALUE;

    public static final Integer DORIS_TABLET_SIZE_MIN = 1;

    public static final Integer DORIS_BATCH_SIZE_DEFAULT = 1024;

    public static final Long DORIS_EXEC_MEM_LIMIT_DEFAULT = 2147483648L;

    public static final Boolean DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT = false;

    public static final Integer DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT = 64;

    public static final String PARSE_NUMBER_FAILED_MESSAGE =
            "Parse '{}' to number failed. Original string is '{}'.";

    public static final String PARSE_BOOL_FAILED_MESSAGE =
            "Parse '{}' to boolean failed. Original string is '{}'.";

    public static final String CONNECT_FAILED_MESSAGE = "Connect to doris {} failed.";

    public static final String ILLEGAL_ARGUMENT_MESSAGE =
            "argument '{}' is illegal, value is '{}'.";

    public static final String SHOULD_NOT_HAPPEN_MESSAGE = "Should not come here.";

    public static final String DORIS_INTERNAL_FAIL_MESSAGE =
            "Doris server '{}' internal failed, status is '{}', error message is '{}'";
}

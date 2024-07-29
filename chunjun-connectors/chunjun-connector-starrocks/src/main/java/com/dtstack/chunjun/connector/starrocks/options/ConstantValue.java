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

package com.dtstack.chunjun.connector.starrocks.options;

public class ConstantValue {

    // sink
    public static final String FIELD_DELIMITER = "\t";
    public static String LINE_DELIMITER = "\n";

    // driver class name
    public static final String DRIVER_CLASS_NAME = "com.mysql.jdbc.Driver";
    public static final String CJ_DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";

    public static final Integer MAX_RETRIES_DEFAULT = 3;
    public static final String WRITE_MODE_DEFAULT = "APPEND";
    public static final Integer BATCH_SIZE_DEFAULT = 1024 * 10;

    // stream load
    public static final Integer HTTP_CHECK_TIMEOUT_DEFAULT = 10 * 1000;
    public static final Integer QUEUE_OFFER_TIMEOUT_DEFAULT = 60 * 1000;
    public static final Integer QUEUE_POLL_TIMEOUT_DEFAULT = 60 * 1000;
    // 50mb, If you need to set a larger value, you need to set a larger taskmanager memory,
    // otherwise OOM may occur.
    public static final Long SINK_BATCH_MAX_BYTES_DEFAULT = 50 * 1024 * 1024L;
    public static final Long SINK_BATCH_MAX_ROWS_DEFAULT = 2048 * 100L;
}

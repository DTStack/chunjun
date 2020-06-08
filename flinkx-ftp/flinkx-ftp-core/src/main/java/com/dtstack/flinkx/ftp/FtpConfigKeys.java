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

package com.dtstack.flinkx.ftp;

/**
 * This class defines configuration keys for FtpReader and FtpWriter
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class FtpConfigKeys {

    public static final String KEY_USERNAME = "username";

    public static final String KEY_PASSWORD = "password";

    public static final String KEY_PROTOCOL = "protocol";

    public static final String KEY_FIELD_DELIMITER = "fieldDelimiter";

    public static final String KEY_PATH = "path";

    public static final String KEY_ENCODING = "encoding";

    public static final String KEY_CONNECT_PATTERN = "connectPattern";

    public static final String KEY_HOST = "host";

    public static final String KEY_PORT = "port";

    public static final String KEY_WRITE_MODE = "writeMode";

    public static final String KEY_IS_FIRST_HEADER = "isFirstLineHeader";

    public static final String KEY_TIMEOUT = "timeout";

    public static final String KEY_MAX_FILE_SIZE = "maxFileSize";

}

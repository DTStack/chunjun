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

package com.dtstack.flinkx.websocket.constants;

/** 定义常量
 * @Company: www.dtstack.com
 * @author kunni@dtstack.com
 */

public class WebSocketConfig {

    public static final int DEFAULT_RETRY_TIME = 5;

    public static final int DEFAULT_RETRY_INTERVAL = 2000;

    public static final int DEFAULT_PRINT_INTERVAL = 100;

    /**
     * 设置一个websocket client失败时的标志
     */
    public static final String KEY_EXIT0 = "exit0";

    /**
     * 以下是reader端读取的key值
     */
    public static final String KEY_WEB_SOCKET_SERVER_URL = "url";

    public static final String KEY_RETRY_TIME = "retry";

    public static final String KEY_RETRY_INTERVAL = "interval";

    public static final String KEY_MESSAGE = "message";

    public static final String KEY_CODEC = "codec";

    public static final String KEY_PARAMS = "params";

}

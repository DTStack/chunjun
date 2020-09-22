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

package com.dtstack.flinkx.socket.constants;

/**
 * Date: 2019/09/20
 * Company: www.dtstack.com
 * @author kunni@dtstack.com
 */

public class SocketCons {


    /**
     * 自定义编解码器所在包名
     * todo 确定自定义类所在包
     */
    public static final String PREFIX_PACKAGE = "";

    /**
     * reader读取的常量
     */
    public static final String KEY_SERVER = "server";
    public static final String KEY_BYTE_BUF_DECODER = "byteBufDecoder";
    public static final String KEY_BINARY_ARRAY_DECODER = "binaryArrayDecoder";
    public static final String KEY_PROPERTIES = "properties";

    /**
     * 编解码需要的参数
     */
    public static final String DECODE_LENGTH = "length";
    public static final String DECODE_MAX_LENGTH = "maxLength";
    public static final String DECODE_DELIMITER = "delimiter";
    public static final String DECODE_OFFSET = "offset";
    public static final String DECODE_FILED_LENGTH = "fieldLength";
    public static final String DECODE_ADJUSTMENT = "adjustment";
    public static final String DECODE_STRIP = "strip";

    public static final int DEFAULT_IDLE_STATE_HANDLER_THREAD = 4;

}

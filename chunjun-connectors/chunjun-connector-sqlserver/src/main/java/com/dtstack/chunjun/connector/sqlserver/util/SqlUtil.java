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

package com.dtstack.chunjun.connector.sqlserver.util;

import java.nio.ByteBuffer;

public class SqlUtil {

    public static long timestampBytesToLong(byte[] hexBytes) {
        return ByteBuffer.wrap(hexBytes).getLong();
    }

    public static byte[] longToTimestampBytes(long longValue) {
        byte[] timestampBytes = new byte[8];
        for (int i = 7; i >= 0; i--) {
            long bit1 = (longValue % 16);
            longValue = longValue / 16;
            long bit2 = (int) (longValue % 16);
            longValue /= 16;
            timestampBytes[i] = (byte) Integer.parseInt(transToBinaryString(bit1, bit2), 2);
        }
        return timestampBytes;
    }

    public static String transToBinaryString(long hexInt1, long hexInt2) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            long bit = hexInt1 % 2;
            hexInt1 /= 2;
            builder.append(bit);
        }
        for (int i = 0; i < 4; i++) {
            long bit = hexInt2 % 2;
            hexInt2 /= 2;
            builder.append(bit);
        }
        return builder.reverse().toString();
    }
}

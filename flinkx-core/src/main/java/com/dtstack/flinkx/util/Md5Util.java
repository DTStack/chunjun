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

package com.dtstack.flinkx.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * @author jiangbo
 * @date 2020/2/13
 */
public class Md5Util {

    private static final char[] DIGITS_LOWER = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
    };

    public static String getMd5(String value) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] bytes = md5.digest(value.getBytes(StandardCharsets.UTF_8));
            return bytes2Hex(bytes);
        } catch (Exception e) {
            throw new RuntimeException("Get md5 error:", e);
        }
    }

    /**
     * bytes数组转16进制String
     *
     * @param data bytes数组
     * @return 转化结果
     */
    private static String bytes2Hex(final byte[] data) {
        final int l = data.length;
        final char[] out = new char[l << 1];
        // two characters form the hex value.
        for (int i = 0, j = 0; i < l; i++) {
            out[j++] = Md5Util.DIGITS_LOWER[(0xF0 & data[i]) >>> 4];
            out[j++] = Md5Util.DIGITS_LOWER[0x0F & data[i]];
        }
        return new String(out);
    }
}

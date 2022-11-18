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

package com.dtstack.chunjun.connector.saphana;

public class SaphanaDialectTest {

    public static void main(String[] args) {
        byte[] a = short2byte(Short.valueOf("220"));
        System.out.println(byte2short(a));
        byte[] b = short2byte(Short.valueOf("120"));
        System.out.println(byte2short(b));
    }

    public static byte[] short2byte(short s) {
        byte[] b = new byte[2];
        for (int i = 0; i < 2; i++) {
            int offset = 16 - (i + 1) * 8; // 因为byte占4个字节，所以要计算偏移量
            b[i] = (byte) ((s >> offset) & 0xff); // 把16位分为2个8位进行分别存储
        }
        return b;
    }

    public static short byte2short(byte[] b) {
        short l = 0;
        for (int i = 0; i < 2; i++) {
            l <<= 8; // <<=和我们的 +=是一样的，意思就是 l = l << 8
            l |= (b[i] & 0xff); // 和上面也是一样的  l = l | (b[i]&0xff)
        }
        return l;
    }
}

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

package com.dtstack.flinkx.connector.hbase14.util;

import org.apache.commons.io.Charsets;

import java.nio.ByteBuffer;

/**
 * Date: 2018/8/28 Company: www.dtstack.com
 *
 * @author xuchao
 */
public class ByteUtils {

    public static boolean toBoolean(final byte[] b) {
        if (b.length != 1) {
            throw new IllegalArgumentException("Array has wrong size: " + b.length);
        }
        return b[0] != (byte) 0;
    }

    public static String byteToString(byte[] bytes) {
        return new String(bytes, Charsets.UTF_8);
    }

    public static byte[] shortToByte4(short i) {
        byte[] targets = new byte[2];
        targets[1] = (byte) (i & 0xFF);
        targets[0] = (byte) (i >> 8 & 0xFF);
        return targets;
    }

    public static Short byte2ToShort(byte[] bytes) {

        if (bytes.length != 2) {
            throw new RuntimeException("byte2ToUnsignedShort input bytes length need == 2");
        }

        short high = (short) (bytes[0] & 0xFF);
        short low = (short) (bytes[1] & 0xFF);
        return (short) (high << 8 | low);
    }

    /*将INT类型转化为10进制byte数组（占4字节）*/
    public static byte[] int2Bytes(int num) {
        byte[] byteNum = new byte[4];
        for (int ix = 0; ix < 4; ++ix) {
            int offset = 32 - (ix + 1) * 8;
            byteNum[ix] = (byte) ((num >> offset) & 0xff);
        }
        return byteNum;
    }

    /**
     * byte数组转换为int整数
     *
     * @param byteNum byte数组
     * @return int整数
     */
    public static int byte4ToInt(byte[] byteNum) {

        if (byteNum.length != 4) {
            throw new RuntimeException("byte4ToInt input bytes length need == 4");
        }

        int num = 0;
        for (int ix = 0; ix < 4; ++ix) {
            num <<= 8;
            num |= (byteNum[ix] & 0xff);
        }
        return num;
    }

    /*将长整形转化为byte数组*/
    public static byte[] long2Bytes(long num) {
        byte[] byteNum = new byte[8];
        for (int ix = 0; ix < 8; ++ix) {
            int offset = 64 - (ix + 1) * 8;
            byteNum[ix] = (byte) ((num >> offset) & 0xff);
        }
        return byteNum;
    }

    /*将byte数组（长度为8）转化为长整形*/
    public static long bytes2Long(byte[] byteNum) {

        if (byteNum.length != 8) {
            throw new RuntimeException("bytes2Long input bytes length need == 8");
        }

        long num = 0;
        for (int ix = 0; ix < 8; ++ix) {
            num <<= 8;
            num |= (byteNum[ix] & 0xff);
        }
        return num;
    }

    public static byte bytes2Byte(byte[] byteNum) {
        if (byteNum.length != 8) {
            throw new RuntimeException("bytes2Byte input bytes length need == 1");
        }

        return byteNum[0];
    }

    /** 将float转化为byte数组，占用4个字节* */
    public static byte[] float2ByteArray(float value) {
        return ByteBuffer.allocate(4).putFloat(value).array();
    }

    /**
     * 将10进制byte数组转化为Float
     *
     * @param b 字节（至少4个字节）
     * @return
     */
    public static float bytes2Float(byte[] b) {
        int l;
        l = b[0];
        l &= 0xff;
        l |= ((long) b[1] << 8);
        l &= 0xffff;
        l |= ((long) b[2] << 16);
        l &= 0xffffff;
        l |= ((long) b[3] << 24);
        return Float.intBitsToFloat(l);
    }

    public static byte[] double2Bytes(double d) {
        long value = Double.doubleToRawLongBits(d);
        byte[] byteRet = new byte[8];
        for (int i = 0; i < 8; i++) {
            byteRet[i] = (byte) ((value >> 8 * i) & 0xff);
        }
        return byteRet;
    }

    public static double bytes2Double(byte[] arr) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value |= ((long) (arr[i] & 0xff)) << (8 * i);
        }
        return Double.longBitsToDouble(value);
    }
}

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

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Reason: Date: 2018/9/10 Company: www.dtstack.com
 *
 * @author xuchao
 */
public class HBaseUtils {

    public static Object convertByte(byte[] hbaseData, String type) {
        if (type == null) {
            return new String(hbaseData);
        }

        switch (type.toLowerCase()) {
            case "boolean":
                return Bytes.toBoolean(hbaseData);

            case "int":
                return Bytes.toInt(hbaseData);

            case "bigint":
                return Bytes.toLong(hbaseData);

            case "tinyint":
            case "byte":
                return ByteUtils.bytes2Byte(hbaseData);

            case "short":
            case "smallint":
                return Bytes.toShort(hbaseData);

            case "char":
            case "varchar":
            case "string":
                return Bytes.toString(hbaseData);

            case "float":
                return Bytes.toFloat(hbaseData);

            case "double":
                return Bytes.toDouble(hbaseData);
            case "decimal":
                return Bytes.toBigDecimal(hbaseData);
            default:
                throw new RuntimeException("not support type of " + type);
        }
    }
}

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

package com.dtstack.chunjun.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * Class Utility
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class ClassUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ClassUtil.class);

    public static final Object LOCK_STR = new Object();

    public static void forName(String clazz, ClassLoader classLoader) {
        synchronized (LOCK_STR) {
            try {
                Class.forName(clazz, true, classLoader);
                DriverManager.setLoginTimeout(10);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static synchronized void forName(String clazz) {
        try {
            Class<?> driverClass = Class.forName(clazz);
            driverClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据字段类型查找对应的基本类型
     *
     * @param type
     * @return
     */
    public static Class<?> typeToClass(String type) {

        // 这部分主要是告诉Class转TypeInformation的方法，字段是Array类型
        String lowerStr = type.toLowerCase().trim();
        if (lowerStr.startsWith("array")) {
            return Array.newInstance(Integer.class, 0).getClass();
        }
        if (lowerStr.startsWith("map")) {
            Map m = new HashMap();
            return m.getClass();
        }

        switch (lowerStr) {
            case "boolean":
            case "bit":
                return Boolean.class;

            case "smallint":
            case "smallintunsigned":
            case "smallserial":
            case "tinyint":
            case "tinyintunsigned":
            case "mediumint":
            case "mediumintunsigned":
            case "integer":
            case "int":
            case "serial":
                return Integer.class;

            case "blob":
            case "bytea":
                return Byte.class;

            case "bigint":
            case "intunsigned":
            case "integerunsigned":
            case "bigintunsigned":
            case "long":
            case "id":
            case "bigserial":
            case "oid":
            case "double precision":
                return Long.class;

            case "varchar":
            case "varchar(max)":
            case "char":
            case "text":
            case "xml":
            case "nchar":
            case "nvarchar":
            case "nvarchar(max)":
            case "ntext":
            case "uniqueidentifier":
            case "string":
            case "character_varying":
            case "character":
            case "name":
                return String.class;

            case "real":
            case "float":
            case "realunsigned":
            case "floatunsigned":
                return Float.class;

            case "double":
            case "doubleunsigned":
                return Double.class;

            case "date":
                return Date.class;

            case "datetime":
            case "datetime2":
            case "smalldatetime":
            case "timestamp":
                return Timestamp.class;

            case "time":
                return Time.class;

            case "decimal":
            case "decimalunsigned":
            case "money":
            case "smallmoney":
            case "numeric":
                return BigDecimal.class;
            default:
                break;
        }

        throw new RuntimeException("不支持 " + type + " 类型");
    }
}

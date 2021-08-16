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

package com.dtstack.flinkx.enums;

import com.dtstack.flinkx.constants.ConstantValue;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Define standard column type for all the readers or writers that do not have special types of
 * their own
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public enum ColumnType {

    /** string type */
    STRING,
    VARCHAR,
    VARCHAR2,
    CHAR,
    NVARCHAR,
    TEXT,
    KEYWORD,
    BINARY,

    /** number type */
    INT,
    INT32,
    MEDIUMINT,
    TINYINT,
    DATETIME,
    SMALLINT,
    BIGINT,
    LONG,
    INT64,
    SHORT,
    INTEGER,
    NUMBER,

    /** double type */
    DOUBLE,
    FLOAT,
    BOOLEAN,

    /** date type */
    DATE,
    TIMESTAMP,
    TIME,
    DECIMAL,
    YEAR,
    BIT;

    public static List<ColumnType> TIME_TYPE = Arrays.asList(DATE, DATETIME, TIME, TIMESTAMP);

    public static List<ColumnType> NUMBER_TYPE =
            Arrays.asList(
                    INT, INTEGER, MEDIUMINT, TINYINT, SMALLINT, BIGINT, LONG, SHORT, DOUBLE, FLOAT,
                    DECIMAL, NUMBER);

    public static List<ColumnType> STRING_TYPE =
            Arrays.asList(STRING, VARCHAR, VARCHAR2, CHAR, NVARCHAR, TEXT, KEYWORD, BINARY);

    /**
     * 根据字段类型的字符串找出对应的枚举 找不到直接报错 IllegalArgumentException
     *
     * @param type
     * @return
     */
    public static ColumnType fromString(String type) {
        if (type == null) {
            throw new RuntimeException("null ColumnType!");
        }

        if (type.contains(ConstantValue.LEFT_PARENTHESIS_SYMBOL)) {
            type = type.substring(0, type.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL));
        }

        type = type.toUpperCase(Locale.ENGLISH);
        // 为了支持无符号类型  如 int unsigned
        if (StringUtils.contains(type, ConstantValue.DATA_TYPE_UNSIGNED)) {
            type = type.replace(ConstantValue.DATA_TYPE_UNSIGNED, "").trim();
        }
        return valueOf(type);
    }

    /**
     * 根据字段类型的字符串找到对应的枚举 找不到就直接返回ColumnType.STRING;
     *
     * @param type
     * @return
     */
    public static ColumnType getType(String type) {
        type = type.toUpperCase(Locale.ENGLISH);
        if (type.contains(ConstantValue.LEFT_PARENTHESIS_SYMBOL)) {
            type = type.substring(0, type.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL));
        }

        // 为了支持无符号类型  如 int unsigned
        if (StringUtils.contains(type, ConstantValue.DATA_TYPE_UNSIGNED)) {
            type = type.replaceAll(ConstantValue.DATA_TYPE_UNSIGNED, "").trim();
        }

        if (type.contains(ColumnType.TIMESTAMP.name())) {
            return TIMESTAMP;
        }

        for (ColumnType value : ColumnType.values()) {
            if (type.equalsIgnoreCase(value.name())) {
                return value;
            }
        }

        return ColumnType.STRING;
    }

    public static boolean isTimeType(String type) {
        return TIME_TYPE.contains(getType(type));
    }

    public static boolean isNumberType(String type) {
        return NUMBER_TYPE.contains(getType(type));
    }

    public static boolean isStringType(String type) {
        return STRING_TYPE.contains(getType(type));
    }

    public static boolean isStringType(ColumnType type) {
        return STRING_TYPE.contains(type);
    }
}

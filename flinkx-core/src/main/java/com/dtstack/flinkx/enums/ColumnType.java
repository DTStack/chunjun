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

import java.util.Arrays;
import java.util.List;

/**
 * Define standard column type for all the readers or writers that do not
 * have special types of their own
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public enum ColumnType {

    /**
     * string type
     */
    STRING, VARCHAR,VARCHAR2, CHAR,NVARCHAR,TEXT,KEYWORD,BINARY,

    /**
     * number type
     */
    INT, INT32, MEDIUMINT, TINYINT, DATETIME, SMALLINT, BIGINT,LONG, INT64 ,SHORT,INTEGER,

    /**
     * double type
     */
    DOUBLE, FLOAT,
    BOOLEAN,

    /**
     * date type
     */
    DATE, TIMESTAMP,TIME,
    DECIMAL,YEAR,BIT;

    public static List<ColumnType> TIME_TYPE = Arrays.asList(
            DATE,DATETIME,TIME,TIMESTAMP
    );

    public static List<ColumnType> NUMBER_TYPE = Arrays.asList(
            INT,INTEGER,MEDIUMINT,TINYINT,SMALLINT, BIGINT,LONG,SHORT,DOUBLE, FLOAT,DECIMAL
    );

    public static ColumnType fromString(String type) {
        if(type == null) {
            throw new RuntimeException("null ColumnType!");
        }

        if(type.contains(ConstantValue.LEFT_PARENTHESIS_SYMBOL)){
            type = type.substring(0, type.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL));
        }

        return valueOf(type.toUpperCase());
    }

    public static ColumnType getType(String type){
        if(type.contains(ConstantValue.LEFT_PARENTHESIS_SYMBOL)){
            type = type.substring(0, type.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL));
        }

        if(type.toLowerCase().contains(ColumnType.TIMESTAMP.name().toLowerCase())){
            return TIMESTAMP;
        }

        for (ColumnType value : ColumnType.values()) {
            if(type.equalsIgnoreCase(value.name())){
                return value;
            }
        }

        return ColumnType.STRING;
    }

    public static boolean isTimeType(String type){
        return TIME_TYPE.contains(getType(type));
    }

    public static boolean isNumberType(String type){
        return NUMBER_TYPE.contains(getType(type));
    }
}

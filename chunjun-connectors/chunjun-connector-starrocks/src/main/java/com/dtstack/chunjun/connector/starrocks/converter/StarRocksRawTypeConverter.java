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
package com.dtstack.chunjun.connector.starrocks.converter;

import com.dtstack.chunjun.constants.ConstantValue;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import org.apache.commons.lang3.StringUtils;

import java.util.Locale;

/**
 * @author lihongwei
 * @date 2022/04/11
 */
public class StarRocksRawTypeConverter {

    public static DataType apply(String type) {
        type = type.toUpperCase(Locale.ENGLISH);
        int left = type.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL);
        int right = type.indexOf(ConstantValue.RIGHT_PARENTHESIS_SYMBOL);
        String leftStr = type;
        String rightStr = null;
        if (left > 0 && right > 0) {
            leftStr = type.substring(0, left);
            rightStr = type.substring(left + 1, type.length() - 1);
        }
        switch (leftStr) {
            case "BOOLEAN":
                return DataTypes.BOOLEAN();
            case "TINYINT":
                if (StringUtils.isNotBlank(rightStr) && rightStr.equals("1")) {
                    return DataTypes.BOOLEAN();
                } else {
                    return DataTypes.TINYINT();
                }
            case "SMALLINT":
                return DataTypes.SMALLINT();
            case "INT":
                return DataTypes.INT();
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "BIGINT":
                return DataTypes.BIGINT();
            case "DECIMAL":
            case "DECIMALV2":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
                if (rightStr != null) {
                    String[] split = rightStr.split(ConstantValue.COMMA_SYMBOL);
                    if (split.length == 2) {
                        return DataTypes.DECIMAL(
                                Integer.parseInt(split[0].trim()),
                                Integer.parseInt(split[1].trim()));
                    }
                }
                return DataTypes.DECIMAL(38, 18);
            case "CHAR":
            case "VARCHAR":
            case "STRING":
            case "LARGEINT":
                return DataTypes.STRING();
            case "DATE":
                return DataTypes.DATE();
            case "DATETIME":
                return DataTypes.TIMESTAMP(0);
            case "HLL":
            case "BITMAP":
            case "JSON":
            default:
                throw new UnsupportedOperationException(type);
        }
    }
}

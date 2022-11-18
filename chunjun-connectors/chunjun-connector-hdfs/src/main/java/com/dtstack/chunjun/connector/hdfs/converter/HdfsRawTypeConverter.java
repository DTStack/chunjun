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
package com.dtstack.chunjun.connector.hdfs.converter;

import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BinaryType;

import java.util.Locale;

public class HdfsRawTypeConverter {
    public static DataType apply(String type) throws UnsupportedTypeException {
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
                return DataTypes.TINYINT();
            case "SMALLINT":
                return DataTypes.SMALLINT();
            case "INT":
                return DataTypes.INT();
            case "BIGINT":
                return DataTypes.BIGINT();
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "DECIMAL":
                if (rightStr != null) {
                    String[] split = rightStr.split(ConstantValue.COMMA_SYMBOL);
                    if (split.length == 2) {
                        return DataTypes.DECIMAL(
                                Integer.parseInt(split[0].trim()),
                                Integer.parseInt(split[1].trim()));
                    }
                }
                return DataTypes.DECIMAL(38, 18);
            case "STRING":
            case "VARCHAR":
            case "CHAR":
                return DataTypes.STRING();
            case "BINARY":
                return DataTypes.BINARY(BinaryType.DEFAULT_LENGTH);
            case "TIMESTAMP":
                if (rightStr != null) {
                    String[] split = rightStr.split(ConstantValue.COMMA_SYMBOL);
                    if (split.length == 1) {
                        return DataTypes.TIMESTAMP(Integer.parseInt(split[0].trim()));
                    }
                }
                return DataTypes.TIMESTAMP(6);
            case "DATE":
                return DataTypes.DATE();
            case "ARRAY":
            case "MAP":
            case "STRUCT":
            case "UNION":
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}

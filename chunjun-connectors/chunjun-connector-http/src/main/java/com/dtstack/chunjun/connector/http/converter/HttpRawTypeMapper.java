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

package com.dtstack.chunjun.connector.http.converter;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.Locale;

public class HttpRawTypeMapper {

    /** 将restapi返回的参数根据定义的类型，转换成flink的DataType类型。 */
    public static DataType apply(TypeConfig type) {
        switch (type.getType()) {
            case "INT":
            case "INTEGER":
                return DataTypes.INT();
            case "TINYINT":
                return DataTypes.TINYINT();
            case "BOOLEAN":
                return DataTypes.BOOLEAN();
            case "SHORT":
                return DataTypes.SMALLINT();
            case "BIGINT":
            case "BIGINTEGER":
            case "LONG":
                return DataTypes.BIGINT();
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DECIMAL":
            case "BIGDECIMAL":
            case "NUMERIC":
                return DataTypes.DECIMAL(38, 18);
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "CHAR":
            case "VARCHAR":
            case "STRING":
            case "CHARACTER":
                return DataTypes.STRING();
            case "DATE":
                return DataTypes.DATE();
            case "TIME":
                return DataTypes.TIME();
            case "TIMESTAMP":
            case "DATETIME":
                return DataTypes.TIMESTAMP();
            case "BYTES":
            case "BINARY":
                return DataTypes.BYTES();
            default:
                throw new UnsupportedTypeException(type);
        }
    }

    /** 将restapi返回的参数根据定义的类型，转换成flink的DataType类型。 case "DATE":和apply不一致 */
    public static DataType applyForHbase(TypeConfig type) {
        switch (type.getType().toUpperCase(Locale.ENGLISH)) {
            case "INT":
            case "INTEGER":
                return DataTypes.INT();
            case "TINYINT":
                return DataTypes.TINYINT();
            case "BOOLEAN":
                return DataTypes.BOOLEAN();
            case "SHORT":
                return DataTypes.SMALLINT();
            case "BIGINT":
            case "BIGINTEGER":
            case "LONG":
                return DataTypes.BIGINT();
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DECIMAL":
            case "BIGDECIMAL":
            case "NUMERIC":
                return DataTypes.DECIMAL(38, 18);
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "CHAR":
            case "VARCHAR":
            case "STRING":
            case "CHARACTER":
                return DataTypes.STRING();
            case "TIME":
                return DataTypes.TIME();
            case "TIMESTAMP":
            case "DATETIME":
            case "DATE":
                return DataTypes.TIMESTAMP();
            case "BYTES":
            case "BINARY":
                return DataTypes.BYTES();
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}

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

package com.dtstack.chunjun.connector.kafka.converter;

import com.dtstack.chunjun.throwable.UnsupportedTypeException;
import com.dtstack.chunjun.util.ColumnTypeUtil;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.Locale;

public class KafkaRawTypeMapping {

    public static DataType apply(String type) {
        ColumnTypeUtil.DecimalInfo decimalInfo = null;
        if (ColumnTypeUtil.isDecimalType(type)) {
            decimalInfo = ColumnTypeUtil.getDecimalInfo(type, null);
            if (decimalInfo != null) {
                type = ColumnTypeUtil.TYPE_NAME;
            }
        }
        switch (type.toUpperCase(Locale.ROOT)) {
            case "INT":
            case "INTEGER":
                return DataTypes.INT();
            case "BOOLEAN":
            case "BIT":
                return DataTypes.BOOLEAN();
            case "TINYINT":
                return DataTypes.TINYINT();
            case "SHORT":
                return DataTypes.SMALLINT();
            case "SMALLINT":
            case "MEDIUMINT":
            case "BIGINT":
            case "LONG":
                return DataTypes.BIGINT();
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DECIMAL":
                assert decimalInfo != null;
                return DataTypes.DECIMAL(decimalInfo.getPrecision(), decimalInfo.getScale());
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "CHAR":
            case "CHARACTER":
            case "STRING":
            case "VARCHAR":
            case "TEXT":
                return DataTypes.STRING();
            case "DATE":
            case "YEAR":
                return DataTypes.DATE();
            case "TIME":
                return DataTypes.TIME();
            case "TIMESTAMP":
            case "DATETIME":
                return DataTypes.TIMESTAMP();

            default:
                throw new UnsupportedTypeException(type);
        }
    }
}

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

package com.dtstack.chunjun.connector.binlog.converter;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

public class MysqlBinlogRawTypeMapper {

    /**
     * 将MySQL数据库中的类型，转换成flink的DataType类型。 转换关系参考 com.mysql.jdbc.MysqlDefs 类里面的信息。
     * com.mysql.jdbc.ResultSetImpl.getObject(int)
     */
    public static DataType apply(TypeConfig type) {
        switch (type.getType()) {
            case "BIT":
                return DataTypes.BOOLEAN();
            case "TINYINT":
                return DataTypes.TINYINT();
            case "SMALLINT":
            case "MEDIUMINT":
            case "INT":
            case "INTEGER":
            case "INT24":
                return DataTypes.INT();
            case "BIGINT":
                return DataTypes.BIGINT();
            case "REAL":
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DECIMAL":
            case "NUMERIC":
                return DataTypes.DECIMAL(38, 18);
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "CHAR":
            case "VARCHAR":
                return DataTypes.STRING();
            case "DATE":
                return DataTypes.DATE();
            case "TIME":
                return DataTypes.TIME();
            case "YEAR":
                return DataTypes.DATE();
            case "TIMESTAMP":
            case "DATETIME":
                return DataTypes.TIMESTAMP();
            case "TINYBLOB":
            case "BLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
                return DataTypes.BYTES();
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
                return DataTypes.STRING();
            case "BINARY":
            case "VARBINARY":
                // BYTES 底层调用的是VARBINARY最大长度
                return DataTypes.BYTES();
            case "JSON":
                return DataTypes.STRING();
            case "ENUM":
            case "SET":
                return DataTypes.STRING();
            case "GEOMETRY":
                return DataTypes.BYTES();

            default:
                throw new UnsupportedTypeException(type);
        }
    }
}

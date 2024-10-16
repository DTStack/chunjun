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

package com.dtstack.chunjun.connector.opengauss.converter;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.opengauss.converter.logical.BitType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.BoolType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.BpcharType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.ByteaType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.CharType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.DateType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.Float4Type;
import com.dtstack.chunjun.connector.opengauss.converter.logical.Float8Type;
import com.dtstack.chunjun.connector.opengauss.converter.logical.Int2Type;
import com.dtstack.chunjun.connector.opengauss.converter.logical.Int4Type;
import com.dtstack.chunjun.connector.opengauss.converter.logical.Int8Type;
import com.dtstack.chunjun.connector.opengauss.converter.logical.JsonType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.JsonbType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.MoneyType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.NameType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.NumericType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.OidType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.PointType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.TextType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.TimeType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.TimestampType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.TimestampTzType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.TimetzType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.UuidType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.VarbitType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.VarcharType;
import com.dtstack.chunjun.connector.opengauss.converter.logical.XmlType;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

public class OpengaussRawTypeMapper {

    /**
     * https://docs-opengauss.osinfra.cn/zh/docs/5.0.0/docs/BriefTutorial/%E6%95%B0%E6%8D%AE%E7%B1%BB%E5%9E%8B.html
     *
     * @param type
     */
    public static DataType apply(TypeConfig type) {
        switch (type.getType()) {
            case "BIT":
                return new AtomicDataType(new BitType(true, LogicalTypeRoot.BOOLEAN, false));
            case "BOOLEAN":
            case "BOOL":
                return DataTypes.BOOLEAN();
            case "SMALLINT":
            case "SMALLSERIAL":
            case "INT2":
            case "INT":
            case "INTEGER":
            case "SERIAL":
            case "INT4":
                return DataTypes.INT();
            case "BIGINT":
            case "BIGSERIAL":
            case "OID":
            case "INT8":
                return DataTypes.BIGINT();
            case "REAL":
            case "FLOAT4":
                return DataTypes.FLOAT();
            case "FLOAT":
            case "DOUBLE PRECISION":
            case "FLOAT8":
                return DataTypes.DOUBLE();
            case "MONEY":
                return new AtomicDataType(new MoneyType(true, LogicalTypeRoot.DOUBLE, false));
            case "DECIMAL":
            case "NUMERIC":
                return type.toDecimalDataType();
            case "CHARACTER VARYING":
            case "VARCHAR":
            case "CHARACTER":
            case "CHAR":
            case "TEXT":
            case "NAME":
            case "BPCHAR":
                return DataTypes.STRING();
                // Binary Data Types
            case "BYTEA":
                return DataTypes.BYTES();
            case "VARBIT":
                return new AtomicDataType(new VarbitType(true, LogicalTypeRoot.VARCHAR, false));
            case "XML":
                return new AtomicDataType(new XmlType(true, LogicalTypeRoot.VARCHAR, false));
            case "UUID":
                return new AtomicDataType(new UuidType(true, LogicalTypeRoot.VARCHAR, false));
            case "POINT":
                return new AtomicDataType(new PointType(true, LogicalTypeRoot.VARCHAR, false));
                // Date/Time Types
            case "ABSTIME":
            case "TIMESTAMP":
            case "TIMESTAMPTZ":
                return type.toTimestampDataType(6);
            case "DATE":
                return DataTypes.DATE();
            case "TIME":
            case "TIMETZ":
                // todo check sync
                return type.toTimeDataType(6);
            case "JSON":
                return new AtomicDataType(new JsonType(true, LogicalTypeRoot.VARCHAR, false));
            case "JSONB":
                return new AtomicDataType(new JsonbType(true, LogicalTypeRoot.VARCHAR, false));
            case "_BIT":
                return new AtomicDataType(new BitType(true, LogicalTypeRoot.VARCHAR, true));
            case "_BOOL":
                return new AtomicDataType(new BoolType(true, LogicalTypeRoot.VARCHAR, true));
            case "_INT2":
                return new AtomicDataType(new Int2Type(true, LogicalTypeRoot.VARCHAR, true));
            case "_INT4":
                return new AtomicDataType(new Int4Type(true, LogicalTypeRoot.VARCHAR, true));
            case "_INT8":
                return new AtomicDataType(new Int8Type(true, LogicalTypeRoot.VARCHAR, true));
            case "_FLOAT4":
                return new AtomicDataType(new Float4Type(true, LogicalTypeRoot.VARCHAR, true));
            case "_FLOAT8":
                return new AtomicDataType(new Float8Type(true, LogicalTypeRoot.VARCHAR, true));
            case "_NUMERIC":
                return new AtomicDataType(new NumericType(true, LogicalTypeRoot.VARCHAR, true));
            case "_TIME":
                return new AtomicDataType(new TimeType(true, LogicalTypeRoot.VARCHAR, true));
            case "_TIMETZ":
                return new AtomicDataType(new TimetzType(true, LogicalTypeRoot.VARCHAR, true));
            case "_TIMESTAMP":
                return new AtomicDataType(new TimestampType(true, LogicalTypeRoot.VARCHAR, true));
            case "_TIMESTAMPTZ":
                return new AtomicDataType(new TimestampTzType(true, LogicalTypeRoot.VARCHAR, true));
            case "_DATE":
                return new AtomicDataType(new DateType(true, LogicalTypeRoot.VARCHAR, true));
            case "_BYTEA":
                return new AtomicDataType(new ByteaType(true, LogicalTypeRoot.VARCHAR, true));
            case "_VARCHAR":
                return new AtomicDataType(new VarcharType(true, LogicalTypeRoot.VARCHAR, true));
            case "_OID":
                return new AtomicDataType(new OidType(true, LogicalTypeRoot.VARCHAR, true));
            case "_BPCHAR":
                return new AtomicDataType(new BpcharType(true, LogicalTypeRoot.VARCHAR, true));
            case "_TEXT":
                return new AtomicDataType(new TextType(true, LogicalTypeRoot.VARCHAR, true));
            case "_MONEY":
                return new AtomicDataType(new MoneyType(true, LogicalTypeRoot.VARCHAR, true));
                //            case "_INTERVAL":
            case "_CHAR":
                return new AtomicDataType(new CharType(true, LogicalTypeRoot.VARCHAR, true));
            case "_VARBIT":
                return new AtomicDataType(new VarbitType(true, LogicalTypeRoot.VARCHAR, true));
            case "_NAME":
                return new AtomicDataType(new NameType(true, LogicalTypeRoot.VARCHAR, true));
            case "_UUID":
                return new AtomicDataType(new UuidType(true, LogicalTypeRoot.VARCHAR, true));
            case "_XML":
                return new AtomicDataType(new XmlType(true, LogicalTypeRoot.VARCHAR, true));
                //            case "_POINT":
                //            case "_JSONB":
                //            case "_JSON":
                //            case "_REF_CURSOR":

                // 以下类型无法支持
                // Enumerated Types

                // Geometric Types
                //            case "LINE":
                //            case "LSEG":
                //            case "BOX":
                //            case "PATH":
                //            case "POLYGON":
                //            case "CIRCLE":

                // Network Address Types

                //
                //                // JSON Types
                //            case "JSONB":
                //            case "JSONPATH":
                //                return DataTypes.STRING();

            default:
                throw new UnsupportedTypeException(type);
        }
    }
}

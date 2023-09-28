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

package com.dtstack.chunjun.connector.solr.converter;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

/**
 * inspried by
 * https://solr.apache.org/guide/7_4/field-types-included-with-solr.html#field-types-included-with-solr
 */
public class SolrRawTypeMapper {

    /**
     * 将 Spahana 数据库中的类型，转换成flink的DataType类型。
     *
     * @link https://data-flair.training/blogs/sql-data-types-in-sap-hana/
     * @param type
     * @return
     */
    public static DataType apply(TypeConfig type) {
        switch (type.getType()) {
            case "DATE":
                return DataTypes.DATE();
            case "TIME":
                return DataTypes.TIME();
            case "SECONDDATE":
                return DataTypes.TIMESTAMP(0);
            case "TIMESTAMP":
                return DataTypes.TIMESTAMP(7);
                // Numeric Data Type
            case "TINYINT":
                return DataTypes.TINYINT();
            case "SMALLINT":
                return DataTypes.SMALLINT();
            case "INTEGER":
                return DataTypes.INT();
            case "BIGINT":
                return DataTypes.BIGINT();
            case "DECIMAL":
                return DataTypes.DECIMAL(38, 18);
            case "SMALLDECIMAL":
                return DataTypes.DECIMAL(16, 0);
            case "REAL":
                return DataTypes.FLOAT();
            case "DOUBLE":
                return DataTypes.DOUBLE();

                // Character String Data Type
            case "VARCHAR":
                return DataTypes.STRING();
            case "NVARCHAR":
                return DataTypes.STRING();
            case "ALPHANUM":
                return DataTypes.STRING();
            case "SHORTTEXT":
                return DataTypes.STRING();

                // Binary Data Type
            case "VARBINARY":
                // update mode 时不支持
                return DataTypes.BYTES();

                // Boolean Data Type
            case "BOOLEAN":
                return DataTypes.BOOLEAN();

                // Large Object (LOB) Data Type
            case "CLOB":
            case "NCLOB":
            case "TEXT":
            case "BINTEXT":
                return new AtomicDataType(new ClobType(true, LogicalTypeRoot.VARCHAR));
            default:
                // Multi-Valued Data Type
                //  - ARRAY
                // Spatial Data Type
                //  - ST_CircularString
                //  - ST_GeometryCollection
                //  - ST_LineString
                //  - ST_MultiLineString
                //  - ST_MultiPoint
                //  - ST_MultiPolygon
                //  - ST_Point
                //  - ST_Polygon

                throw new UnsupportedTypeException(type);
        }
    }
}

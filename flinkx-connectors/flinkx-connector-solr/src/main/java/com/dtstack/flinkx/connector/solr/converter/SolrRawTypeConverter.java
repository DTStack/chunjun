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

package com.dtstack.flinkx.connector.solr.converter;

import com.dtstack.flinkx.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.Locale;

/**
 * inspried by
 * https://solr.apache.org/guide/7_4/field-types-included-with-solr.html#field-types-included-with-solr
 *
 * @author wuren
 * @program flinkx
 * @create 2021/05/31
 */
public class SolrRawTypeConverter {

    public static DataType apply(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BOOL":
                return DataTypes.BOOLEAN();
            case "INT":
                return DataTypes.INT();
            case "LONG":
                return DataTypes.BIGINT();
            case "STRING":
            case "TEXT":
                return DataTypes.STRING();
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "DATE":
                // todo Date type and accuracy need to be further confirmed
                return DataTypes.DATE();
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}

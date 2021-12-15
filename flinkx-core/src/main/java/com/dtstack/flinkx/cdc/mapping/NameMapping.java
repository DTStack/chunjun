/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.dtstack.flinkx.cdc.mapping;

import com.dtstack.flinkx.element.ColumnRowData;

import com.dtstack.flinkx.element.column.StringColumn;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.Map;

/**
 * 名称匹配.
 *
 * @author shitou
 * @date 2021/12/15
 */
public class NameMapping implements Mapping, Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, String> mappings;

    public NameMapping(Map<String, String> mappings) {
        this.mappings = mappings;
    }

    @Override
    public RowData map(RowData value) {

        // Dml
        if (value instanceof ColumnRowData) {

            Map<String, Integer> identityIndex = getIdentityIndex(value);

            Integer tableIndex = identityIndex.get("table");
            Integer schemaIndex = identityIndex.get("schema");

            String table = value.getString(identityIndex.get("table")).toString();
            String schema = "";
            if (schemaIndex != null) {
                schema = value.getString(schemaIndex).toString();
            }

            String tableIdentity = "".equals(schema) ? table : schema + "." + table;

            if (mappings.containsKey(tableIdentity)) {
                String newName = mappings.get(tableIdentity);
                String[] split = newName.split("\\.");
                ((ColumnRowData) value).setField(tableIndex, new StringColumn(split[0]));
                if (split.length == 2) {
                    if (schemaIndex != null) {
                        ((ColumnRowData) value).setField(schemaIndex, new StringColumn(split[1]));
                    } else {
                        ((ColumnRowData) value).addField(new StringColumn(split[1]));
                        ((ColumnRowData) value).addHeader("schema");
                    }
                    // TODO sqlserver表名可以包含"."
                }
            }
            return value;
        }
        // ddl
        return value;
    }
}

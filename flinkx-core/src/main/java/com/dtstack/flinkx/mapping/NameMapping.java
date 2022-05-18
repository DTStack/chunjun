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

package com.dtstack.flinkx.mapping;

import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.StringColumn;

import org.apache.flink.table.data.RowData;

import org.apache.commons.collections.MapUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.constants.CDCConstantValue.SCHEMA;
import static com.dtstack.flinkx.constants.CDCConstantValue.TABLE;

/**
 * 名称匹配.
 *
 * @author shitou
 * @date 2021/12/15
 */
public class NameMapping implements Mapping<RowData>, Serializable {

    private static final long serialVersionUID = 1L;

    private final NameMappingRule mappingRule;

    public NameMapping(NameMappingConf conf) {
        this.mappingRule = new NameMappingRule(conf);
    }

    @Override
    public RowData map(RowData rowData) {

        // Dml
        if (rowData instanceof ColumnRowData) {
            Map<String, Integer> identityIndex = getIdentityIndex(rowData);
            Integer tableIndex = identityIndex.get(TABLE);
            Integer schemaIndex = identityIndex.get(SCHEMA);

            String table = rowData.getString(identityIndex.get(TABLE)).toString();
            String schema = rowData.getString(identityIndex.get(SCHEMA)).toString();

            String targetSchema = mappingRule.schemaMapping(schema);
            String targetTable = mappingRule.tableMapping(schema, table);

            Map<String, String> mapFields = mappingRule.getMapFields(schema, table);
            if (MapUtils.isNotEmpty(mapFields)) {
                List<String> fields = getFields(rowData);
                fields.forEach(
                        filed -> {
                            String targetField = mappingRule.fieldMapping(filed, mapFields);
                            ((ColumnRowData) rowData).replaceHeader(filed, targetField);
                        });
            }

            ((ColumnRowData) rowData).setField(tableIndex, new StringColumn(targetTable));
            ((ColumnRowData) rowData).setField(schemaIndex, new StringColumn(targetSchema));

            return rowData;
        }
        // ddl
        return rowData;
    }
}

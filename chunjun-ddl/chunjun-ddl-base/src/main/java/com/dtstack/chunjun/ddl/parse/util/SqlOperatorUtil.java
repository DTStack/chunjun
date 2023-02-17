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

package com.dtstack.chunjun.ddl.parse.util;

import com.dtstack.chunjun.cdc.ddl.definition.ColumnDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.ConstraintDefinition;
import com.dtstack.chunjun.cdc.ddl.definition.IndexDefinition;
import com.dtstack.chunjun.mapping.Casing;
import com.dtstack.chunjun.mapping.MappingRule;

import java.util.ArrayList;
import java.util.List;

public class SqlOperatorUtil {

    public static List<ColumnDefinition> mappingColumnDefinition(
            List<ColumnDefinition> columnDefinitionList, MappingRule mappingRule) {
        if (mappingRule != null
                && mappingRule.getCasing() != null
                && mappingRule.getCasing() != Casing.UNCHANGE
                && columnDefinitionList != null) {
            columnDefinitionList.forEach(
                    columnDefinition ->
                            columnDefinition.setName(
                                    mappingRule.casingName(columnDefinition.getName())));
        }
        return columnDefinitionList;
    }

    public static List<IndexDefinition> mappingIndexDefinition(
            List<IndexDefinition> indexDefinitionList, MappingRule mappingRule) {
        if (mappingRule != null
                && mappingRule.getCasing() != null
                && mappingRule.getCasing() != Casing.UNCHANGE
                && indexDefinitionList != null) {
            indexDefinitionList.forEach(
                    indexDefinition -> {
                        if (indexDefinition.getColumns() != null) {
                            indexDefinition
                                    .getColumns()
                                    .forEach(
                                            columnInfo ->
                                                    columnInfo.setName(
                                                            mappingRule.casingName(
                                                                    columnInfo.getName())));
                        }
                    });
        }
        return indexDefinitionList;
    }

    public static List<ConstraintDefinition> mappingConstraintDefinition(
            List<ConstraintDefinition> constraintDefinitionList, MappingRule mappingRule) {
        if (mappingRule != null
                && mappingRule.getCasing() != null
                && mappingRule.getCasing() != Casing.UNCHANGE
                && constraintDefinitionList != null) {
            constraintDefinitionList.forEach(
                    constraintDefinition -> {
                        if (constraintDefinition.getColumns() != null) {
                            List<String> columnList = new ArrayList<>();
                            constraintDefinition
                                    .getColumns()
                                    .forEach(
                                            column ->
                                                    columnList.add(mappingRule.casingName(column)));
                            constraintDefinition.setColumns(columnList);
                        }
                    });
        }
        return constraintDefinitionList;
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dtstack.chunjun.mapping;

import com.dtstack.chunjun.cdc.DdlRowData;
import com.dtstack.chunjun.cdc.ddl.DdlConvent;
import com.dtstack.chunjun.cdc.ddl.DdlRowDataConvented;
import com.dtstack.chunjun.cdc.ddl.definition.DdlOperator;
import com.dtstack.chunjun.cdc.ddl.definition.TableIdentifier;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.element.column.StringColumn;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.dtstack.chunjun.constants.CDCConstantValue.DATABASE;
import static com.dtstack.chunjun.constants.CDCConstantValue.SCHEMA;
import static com.dtstack.chunjun.constants.CDCConstantValue.TABLE;

public class NameMapping implements Mapping<RowData, RowData>, Serializable {

    public static final long serialVersionUID = 1L;

    private final MappingRule mappingRule;
    private DdlConvent sourceDdlConvent;
    private DdlConvent sinkDdlConvent;
    private Boolean useDdlConvent;

    public NameMapping(
            MappingConfig conf,
            Boolean useDdlConvent,
            DdlConvent sourceDdlConvent,
            DdlConvent sinkDdlConvent) {
        if (conf != null) {
            this.mappingRule = new MappingRule(conf);
        } else {
            mappingRule = null;
        }
        this.useDdlConvent = useDdlConvent;
        this.sourceDdlConvent = sourceDdlConvent;
        this.sinkDdlConvent = sinkDdlConvent;
    }

    @Override
    public List<RowData> map(RowData rowData) {

        // Dml
        if (rowData instanceof ColumnRowData) {
            Map<String, Integer> identityIndex = getIdentityIndex(rowData);
            Integer tableIndex = identityIndex.get(TABLE);
            Integer schemaIndex = identityIndex.get(SCHEMA);
            Integer dataBaseIndex = identityIndex.get(DATABASE);

            String table = rowData.getString(tableIndex).toString();
            String schema = rowData.getString(schemaIndex).toString();
            String dataBase = null;
            if (((ColumnRowData) rowData).getField(dataBaseIndex) != null
                    && ((ColumnRowData) rowData).getField(dataBaseIndex).getData() != null) {
                dataBase = rowData.getString(dataBaseIndex).toString();
            }

            if (mappingRule != null) {
                TableIdentifier originalIdentifier = new TableIdentifier(dataBase, schema, table);
                TableIdentifier tableIdentifier =
                        mappingRule.tableIdentifierMapping(originalIdentifier);

                if (mappingRule.getCasing() != Casing.UNCHANGE) {
                    tableIdentifier = mappingRule.casingTableIdentifier(tableIdentifier);

                    Set<String> extHeader = ((ColumnRowData) rowData).getExtHeader();
                    for (String fieldName :
                            ((ColumnRowData) rowData)
                                    .getHeaderInfo().keySet().stream()
                                            .filter(fieldName -> !extHeader.contains(fieldName))
                                            .collect(Collectors.toList())) {
                        ((ColumnRowData) rowData)
                                .replaceHeader(fieldName, mappingRule.casingName(fieldName));
                    }
                }

                ((ColumnRowData) rowData)
                        .setField(tableIndex, new StringColumn(tableIdentifier.getTable()));
                ((ColumnRowData) rowData)
                        .setField(schemaIndex, new StringColumn(tableIdentifier.getSchema()));
                if (tableIdentifier.getDataBase() != null) {
                    ((ColumnRowData) rowData)
                            .setField(
                                    dataBaseIndex, new StringColumn(tableIdentifier.getDataBase()));
                } else {
                    ((ColumnRowData) rowData).setField(dataBaseIndex, new NullColumn());
                }
            }

            return Collections.singletonList(rowData);
        } else if (rowData instanceof DdlRowData) {

            DdlRowData ddlRowData = (DdlRowData) rowData;
            DdlRowData replaceMeta = ddlRowData.copy();

            try {
                // 元数据信息替换
                if (null != mappingRule) {
                    TableIdentifier originalIdentifier = replaceMeta.getTableIdentifier();
                    TableIdentifier tableIdentifier =
                            mappingRule.tableIdentifierMapping(originalIdentifier);
                    if (mappingRule.getCasing() != Casing.UNCHANGE) {
                        tableIdentifier = mappingRule.casingTableIdentifier(tableIdentifier);
                    }

                    replaceMeta.setDdlInfo("database", tableIdentifier.getDataBase());
                    replaceMeta.setDdlInfo("schema", tableIdentifier.getSchema());
                    replaceMeta.setDdlInfo("table", tableIdentifier.getTable());
                }

                // ddl转换
                if (useDdlConvent) {
                    if ((sourceDdlConvent == null || sinkDdlConvent == null)) {
                        return Collections.singletonList(
                                new DdlRowDataConvented(
                                        ddlRowData,
                                        new RuntimeException(
                                                "ddlConvent plugin is not exists, the SQL needs to be executed manually downstream"),
                                        null));
                    } else {
                        // ddl的元数据信息进行替换
                        List<String> sql = sourceDdlConvent.map(ddlRowData);

                        // 只有异构数据源才需要sql解析转为sink端语法sql
                        if (sourceDdlConvent
                                .getDataSourceType()
                                .equals(sinkDdlConvent.getDataSourceType())) {
                            return Collections.singletonList(
                                    new DdlRowDataConvented(replaceMeta, null, sql.get(0)));
                        }

                        DdlRowData copyDdlRowData = replaceMeta.copy();
                        copyDdlRowData.setDdlInfo("content", sql.get(0));
                        List<DdlOperator> ddlOperators =
                                sourceDdlConvent.rowConventToDdlData(copyDdlRowData);
                        ArrayList<RowData> ddlRowDataList = new ArrayList<>(ddlOperators.size());
                        for (int i = 0; i < ddlOperators.size(); i++) {
                            List<String> conventSqlList =
                                    sinkDdlConvent.ddlDataConventToSql(ddlOperators.get(i));
                            for (int j = 0; j < conventSqlList.size(); j++) {
                                DdlRowData copy = replaceMeta.copy();
                                copy.setDdlInfo("lsn_sequence", copy.getLsnSequence() + i + j + "");
                                ddlRowDataList.add(
                                        new DdlRowDataConvented(copy, null, conventSqlList.get(j)));
                            }
                        }
                        return ddlRowDataList;
                    }
                }
                return Collections.singletonList(replaceMeta);
            } catch (Throwable e) {
                return Collections.singletonList(new DdlRowDataConvented(replaceMeta, e, null));
            }
        }
        return Collections.singletonList(rowData);
    }
}

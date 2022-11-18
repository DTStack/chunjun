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

package com.dtstack.chunjun.mapping;

import com.dtstack.chunjun.cdc.ddl.definition.TableIdentifier;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

public class MappingRule implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String META_DATABASE_AME = "${dataBaseName}";
    private static final String META_SCHEMA_AME = "${schemaName}";
    private static final String META_TABLE_AME = "${tableName}";

    private final Map<Pattern, TableIdentifier> identifierMappings;
    private final Map<String, String> columnTypeMappings;

    private final Casing casing;

    public MappingRule(MappingConfig conf) {
        identifierMappings = new LinkedHashMap<>(32);
        columnTypeMappings = new LinkedHashMap<>(32);
        if (MapUtils.isNotEmpty(conf.getIdentifierMappings())) {
            conf.getIdentifierMappings()
                    .forEach(
                            (k, v) -> {
                                identifierMappings.put(
                                        Pattern.compile(k), conventToTableIdentifier(v));
                            });
        }

        if (MapUtils.isNotEmpty(conf.getColumnTypeMappings())) {
            conf.getColumnTypeMappings()
                    .forEach(
                            (k, v) -> {
                                columnTypeMappings.put(k.toUpperCase(Locale.ENGLISH), v);
                            });
        }

        casing =
                Casing.valueOf(
                        conf.getCasing() == null
                                ? Casing.UNCHANGE.name()
                                : conf.getCasing().toUpperCase(Locale.ENGLISH));
    }

    public TableIdentifier tableIdentifierMapping(TableIdentifier tableIdentifier) {

        for (Map.Entry<Pattern, TableIdentifier> patternStringEntry :
                identifierMappings.entrySet()) {
            if (patternStringEntry
                    .getKey()
                    .matcher(tableIdentifier.tableIdentifier("."))
                    .matches()) {
                TableIdentifier value = patternStringEntry.getValue();

                TableIdentifier copy = tableIdentifier;
                if (tableIdentifier.getDataBase() == null || tableIdentifier.getSchema() == null) {
                    if (tableIdentifier.getDataBase() == null) {
                        copy =
                                new TableIdentifier(
                                        tableIdentifier.getSchema(),
                                        tableIdentifier.getSchema(),
                                        tableIdentifier.getTable());
                    }
                    if (tableIdentifier.getSchema() == null) {
                        copy =
                                new TableIdentifier(
                                        tableIdentifier.getDataBase(),
                                        tableIdentifier.getDataBase(),
                                        tableIdentifier.getTable());
                    }
                }

                String dataBase = build(value.getDataBase(), copy);
                String schema = build(value.getSchema(), copy);
                String table = build(value.getTable(), copy);

                return new TableIdentifier(dataBase, schema, table);
            }
        }
        return tableIdentifier;
    }

    public String mapType(String original) {
        return columnTypeMappings.getOrDefault(original.toUpperCase(Locale.ENGLISH), original);
    }

    private String build(String replace, TableIdentifier original) {
        if (StringUtils.isBlank(replace)) {
            return replace;
        }
        if (original.getDataBase() != null) {
            replace = replace.replace(META_DATABASE_AME, original.getDataBase());
        }
        if (original.getSchema() != null) {
            replace = replace.replace(META_SCHEMA_AME, original.getSchema());
        }
        if (original.getTable() != null) {
            replace = replace.replace(META_TABLE_AME, original.getTable());
        }
        return replace;
    }

    private TableIdentifier conventToTableIdentifier(String original) {
        ArrayList<String> strings = new ArrayList<>();
        char[] chars = original.toCharArray();
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < chars.length; i++) {
            if (chars[i] == '\\') {
                stringBuilder.append(chars[i]);
                if (chars[++i] == '\\') {
                    stringBuilder.append(chars[i]);
                    if (chars[++i] == '.') {
                        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
                        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
                        stringBuilder.append(chars[i]);
                        continue;
                    }
                }
            }
            if (chars[i] == '.') {
                strings.add(stringBuilder.toString());
                stringBuilder = new StringBuilder();
            } else {
                stringBuilder.append(chars[i]);
            }
            if (i == chars.length - 1) {
                strings.add(stringBuilder.toString());
            }
        }

        if (strings.size() == 2) {
            return new TableIdentifier(null, strings.get(0), strings.get(1));
        } else if (strings.size() == 3) {
            return new TableIdentifier(strings.get(0), strings.get(1), strings.get(2));
        } else if (strings.size() == 1) {
            return new TableIdentifier(null, strings.get(0), null);
        }
        throw new IllegalArgumentException("parse mapping conf error 【" + original + "】");
    }

    public String casingName(String columnName) {
        if (casing == null) {
            return columnName;
        }
        switch (casing) {
            case UNCHANGE:
                return columnName;
            case LOWER:
                return columnName.toLowerCase(Locale.ENGLISH);
            case UPPER:
                return columnName.toUpperCase(Locale.ENGLISH);
            default:
                throw new UnsupportedOperationException();
        }
    }

    public TableIdentifier casingTableIdentifier(TableIdentifier tableIdentifier) {
        if (this.getCasing() != null && this.getCasing() != Casing.UNCHANGE) {
            return new TableIdentifier(
                    tableIdentifier.getDataBase() == null
                            ? null
                            : this.casingName(tableIdentifier.getDataBase()),
                    tableIdentifier.getSchema() == null
                            ? null
                            : this.casingName(tableIdentifier.getSchema()),
                    tableIdentifier.getTable() == null
                            ? null
                            : this.casingName(tableIdentifier.getTable()));
        }
        return tableIdentifier;
    }

    public Casing getCasing() {
        return casing;
    }
}

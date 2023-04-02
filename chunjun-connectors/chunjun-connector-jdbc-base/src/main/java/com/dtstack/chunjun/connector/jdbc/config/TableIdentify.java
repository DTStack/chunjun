/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.jdbc.conf;

import org.apache.commons.lang3.StringUtils;

import java.util.function.Function;

public class TableIdentify {
    private String catalog;
    private String schema;
    private String table;
    private final Function<String, String> quoteIdentifier;
    private final boolean quoteForGetIndex;

    public TableIdentify(
            String catalog,
            String schema,
            String table,
            Function<String, String> quoteIdentifier,
            boolean quoteForGetIndex) {
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
        this.quoteIdentifier = quoteIdentifier;
        this.quoteForGetIndex = quoteForGetIndex;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getQuotedCatalog() {
        return quote(catalog);
    }

    public String getCatalogForGetIndex() {
        return quoteForGetIndex ? quote(catalog) : catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getSchema() {
        return schema;
    }

    public String getQuotedSchema() {
        return quote(schema);
    }

    public String getSchemaForGetIndex() {
        return quoteForGetIndex ? quote(schema) : schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public String getQuotedTable() {
        return quote(table);
    }

    public String getTableForGetIndex() {
        return quoteForGetIndex ? quote(table) : table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getTableInfo() {
        String result = quoteIdentifier.apply(table);
        if (schema != null) {
            result = quoteIdentifier.apply(schema) + "." + result;
        } else if (catalog != null) {
            result = quoteIdentifier.apply(catalog) + "." + result;
        }
        return result;
    }

    private String quote(String identity) {
        if (StringUtils.isEmpty(identity)) {
            return identity;
        }
        return quoteIdentifier.apply(identity);
    }
}

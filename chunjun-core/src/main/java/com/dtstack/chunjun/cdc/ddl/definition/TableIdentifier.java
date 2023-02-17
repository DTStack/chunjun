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

package com.dtstack.chunjun.cdc.ddl.definition;

import java.io.Serializable;
import java.util.Objects;

public class TableIdentifier implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String dataBase;
    private final String schema;
    private final String table;

    public TableIdentifier(String dataBase, String schema, String table) {
        this.dataBase = dataBase;
        this.schema = schema;
        this.table = table;
    }

    public String getDataBase() {
        return dataBase;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String tableIdentifier() {
        return tableIdentifier("_");
    }

    public String tableIdentifier(String split) {
        StringBuilder builder = new StringBuilder();
        if (Objects.nonNull(dataBase)) {
            builder.append(dataBase);
        }

        if (Objects.nonNull(schema)) {
            if (builder.length() > 0) {
                builder.append(split).append(schema);
            } else {
                builder.append(schema);
            }
        }

        if (Objects.nonNull(table)) {
            if (builder.length() > 0) {
                builder.append(split).append(table);
            } else {
                builder.append(table);
            }
        }
        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableIdentifier)) return false;
        TableIdentifier that = (TableIdentifier) o;
        return Objects.equals(dataBase, that.dataBase)
                && Objects.equals(schema, that.schema)
                && Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataBase, schema, table);
    }

    public static class Builder {
        private String database;

        private String schema;

        private String table;

        public Builder setDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder setSchema(String schema) {
            this.schema = schema;
            return this;
        }

        public Builder setTable(String table) {
            this.table = table;
            return this;
        }

        public TableIdentifier build() {
            return new TableIdentifier(database, schema, table);
        }
    }

    @Override
    public String toString() {
        return "TableIdentifier{"
                + "dataBase='"
                + dataBase
                + '\''
                + ", schema='"
                + schema
                + '\''
                + ", table='"
                + table
                + '\''
                + '}';
    }
}

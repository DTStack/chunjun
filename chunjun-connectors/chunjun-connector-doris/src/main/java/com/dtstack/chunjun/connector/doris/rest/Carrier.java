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

package com.dtstack.chunjun.connector.doris.rest;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.IntStream;

@Data
public class Carrier implements Serializable {
    private static final long serialVersionUID = 1L;
    private final List<Map<String, Object>> insertContent = new ArrayList<>();
    private final StringJoiner deleteContent = new StringJoiner(" OR ");
    private int batch = 0;
    private String database;
    private String table;
    private List<String> columns;
    private final Set<Integer> rowDataIndexes = new HashSet<>();

    public String getDatabase() {
        return database;
    }

    public String getDeleteContent() {
        return deleteContent.toString();
    }

    public void addRowDataIndex(int index) {
        rowDataIndexes.add(index);
    }

    public void addInsertContent(List<String> insertV) {
        if (!insertV.isEmpty()) {
            if (insertV.size() > columns.size()) {
                // It is certain that in this case, the size
                // of insertV is twice the size of column
                List<String> forward = insertV.subList(0, columns.size());
                final Map<String, Object> forwardV = new HashMap<>(columns.size());
                IntStream.range(0, columns.size())
                        .forEach(i -> forwardV.put(columns.get(i), forward.get(i)));
                insertContent.add(forwardV);
                List<String> behind = insertV.subList(columns.size(), insertV.size());
                final Map<String, Object> behindV = new HashMap<>(columns.size());
                IntStream.range(0, columns.size())
                        .forEach(i -> behindV.put(columns.get(i), behind.get(i)));
                insertContent.add(behindV);
            } else {
                final Map<String, Object> values = new HashMap<>(columns.size());
                IntStream.range(0, columns.size())
                        .forEach(i -> values.put(columns.get(i), insertV.get(i)));
                insertContent.add(values);
            }
        }
    }

    public void addDeleteContent(List<String> deleteV) {
        if (!deleteV.isEmpty()) {
            String s = buildMergeOnConditions(columns, deleteV);
            deleteContent.add(s);
        }
    }

    public void updateBatch() {
        batch++;
    }

    /**
     * Construct the Doris delete on condition, which only takes effect in the merge http request.
     *
     * @return delete on condition
     */
    private String buildMergeOnConditions(List<String> columns, List<String> values) {
        List<String> deleteOnStr = new ArrayList<>();
        for (int i = 0, size = columns.size(); i < size; i++) {
            String s =
                    "`"
                            + columns.get(i)
                            + "`"
                            + "<=>"
                            + "'"
                            + ((values.get(i)) == null ? "" : values.get(i))
                            + "'";
            deleteOnStr.add(s);
        }
        return StringUtils.join(deleteOnStr, " AND ");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{database:");
        sb.append(database);
        sb.append(", table:");
        sb.append(table);
        sb.append(", columns:[");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(columns.get(i));
        }
        sb.append("], insert_value:");
        sb.append(insertContent);
        sb.append(", delete_value:");
        sb.append(deleteContent);
        sb.append(", batch:");
        sb.append(batch);
        sb.append("}");
        return sb.toString();
    }
}

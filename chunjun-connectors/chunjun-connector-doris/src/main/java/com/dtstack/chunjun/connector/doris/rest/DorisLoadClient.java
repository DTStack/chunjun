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

package com.dtstack.chunjun.connector.doris.rest;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.doris.DorisUtil;
import com.dtstack.chunjun.connector.doris.options.DorisConfig;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.throwable.WriteRecordException;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import lombok.AllArgsConstructor;
import org.apache.commons.collections.MapUtils;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@AllArgsConstructor
public class DorisLoadClient implements Serializable {

    private static final long serialVersionUID = 9155539896112185387L;

    private final Set<String> metaHeader =
            Stream.of("schema", "table", "type", "opTime", "ts", "scn")
                    .collect(Collectors.toCollection(HashSet::new));
    private static final String KEY_SCHEMA = "schema";
    private static final String KEY_TABLE = "table";
    private static final String KEY_POINT = ".";
    public static final String KEY_BEFORE = "before_";
    public static final String KEY_AFTER = "after_";

    private final DorisStreamLoad dorisStreamLoad;
    private final boolean nameMapped;
    private final DorisConfig config;

    public void process(RowData value, List<String> columns, AbstractRowConverter converter)
            throws Exception {
        String schema;
        String table;
        List<String> insertV = new LinkedList<>();
        List<String> deleteV = new LinkedList<>();
        // sync job.
        if (value instanceof ColumnRowData) {
            List<String> columnsFromValue = new LinkedList<>();
            Map<String, String> identityMap = new HashMap<>(2);
            wrap((ColumnRowData) value, columnsFromValue, insertV, deleteV, identityMap);
            schema = MapUtils.getString(identityMap, KEY_SCHEMA, config.getDatabase());
            table = MapUtils.getString(identityMap, KEY_TABLE, config.getTable());
            Carrier carrier = initCarrier(columnsFromValue, insertV, deleteV, schema, table);
            flush(carrier);
        }

        if (value instanceof GenericRowData) {
            schema = config.getDatabase();
            table = config.getTable();
            String[] joiner = new String[columns.size()];
            if (RowKind.INSERT.equals(value.getRowKind())) {
                String[] split = (String[]) converter.toExternal(value, joiner);
                insertV.addAll(Arrays.asList(split));
            }
            Carrier carrier = initCarrier(columns, insertV, deleteV, schema, table);
            flush(carrier);
        }
    }

    /**
     * Each time a RowData is processed, after a Carrier is obtained , it is added to the cache ,
     * and the position of each RowData is recorded.
     *
     * @param value RowData
     * @param index RowData index
     * @param carrierMap A batch of data is cached
     */
    public void process(
            RowData value,
            int index,
            Map<String, Carrier> carrierMap,
            List<String> columns,
            AbstractRowConverter converter)
            throws Exception {
        List<String> insertV = new LinkedList<>();
        List<String> deleteV = new LinkedList<>();
        if (value instanceof ColumnRowData) {
            processWithColumnRowData(value, index, carrierMap, deleteV, insertV);
        }

        if (value instanceof GenericRowData) {
            processWithGenericRowData(
                    value, index, carrierMap, deleteV, insertV, columns, converter);
        }
    }

    /**
     * process column row data.
     *
     * @param value row data
     * @param index index
     * @param carrierMap carrier map
     * @param deleteV delete value.
     * @param insertV insert value.
     */
    private void processWithColumnRowData(
            RowData value,
            int index,
            Map<String, Carrier> carrierMap,
            List<String> deleteV,
            List<String> insertV) {
        Map<String, String> identityMap = new HashMap<>(2);
        List<String> columns = new LinkedList<>();
        wrap((ColumnRowData) value, columns, insertV, deleteV, identityMap);
        String schema = MapUtils.getString(identityMap, KEY_SCHEMA, config.getDatabase());
        String table = MapUtils.getString(identityMap, KEY_TABLE, config.getTable());
        String key = schema + KEY_POINT + table;
        if (carrierMap.containsKey(key)) {
            Carrier carrier = carrierMap.get(key);
            carrier.addInsertContent(insertV);
            carrier.addDeleteContent(deleteV);
            carrier.addRowDataIndex(index);
            carrier.updateBatch();
        } else {
            Carrier carrier = initCarrier(columns, insertV, deleteV, schema, table);
            carrier.addRowDataIndex(index);
            carrierMap.put(key, carrier);
        }
    }

    private void processWithGenericRowData(
            RowData value,
            int index,
            Map<String, Carrier> carrierMap,
            List<String> deleteV,
            List<String> insertV,
            List<String> columns,
            AbstractRowConverter converter)
            throws Exception {
        String schema = config.getDatabase();
        String table = config.getTable();
        String[] joiner = new String[columns.size()];
        if (RowKind.INSERT.equals(value.getRowKind())) {
            String[] split = (String[]) converter.toExternal(value, joiner);
            insertV.addAll(Arrays.asList(split));
        }
        String key = schema + KEY_POINT + table;
        if (carrierMap.containsKey(key)) {
            Carrier carrier = carrierMap.get(key);
            carrier.addInsertContent(insertV);
            carrier.addDeleteContent(deleteV);
            carrier.addRowDataIndex(index);
            carrier.updateBatch();
        } else {
            Carrier carrier = initCarrier(columns, insertV, deleteV, schema, table);
            carrier.addRowDataIndex(index);
            carrierMap.put(key, carrier);
        }
    }

    /**
     * flush data to doris BE.
     *
     * @param carrier data carrier
     * @throws WriteRecordException
     */
    public void flush(final Carrier carrier) throws WriteRecordException {
        try {
            DorisUtil.doRetry(
                    dorisStreamLoad::load,
                    dorisStreamLoad::replaceBackend,
                    carrier,
                    config.getMaxRetries(),
                    config.getWaitRetryMills());
        } catch (Exception e) {
            String errorMessage = "write record failed.";
            throw new WriteRecordException(errorMessage, e, -1, carrier.toString());
        }
    }

    /**
     * Obtain column name, insert values, delete values, and schema and table from ColumnRowData
     *
     * @param value ColumnRowData
     * @param columns column name list
     * @param insertV insert value list
     * @param deleteV delete value list
     * @param identityMap schema and table name
     */
    private void wrapColumnsFromRowData(
            ColumnRowData value,
            List<String> columns,
            List<String> insertV,
            List<String> deleteV,
            Map<String, String> identityMap,
            boolean delete) {

        String[] headers = value.getHeaders();
        Integer schemaIndex = null, tableIndex = null;
        boolean hasBefore = false, hasAfter = false;
        // obtain the schema, table and values.
        for (int i = 0; i < Objects.requireNonNull(headers).length; i++) {
            if (KEY_SCHEMA.equalsIgnoreCase(headers[i])) {
                schemaIndex = i;
                continue;
            }
            if (KEY_TABLE.equalsIgnoreCase(headers[i])) {
                tableIndex = i;
                continue;
            }

            // is column.
            if (!metaHeader.contains(headers[i])) {
                // case 1, need to delete.
                if (headers[i].startsWith(KEY_BEFORE)) {
                    String column = headers[i].substring(7);
                    hasBefore = true;
                    if (!hasAfter) {
                        columns.add(column);
                    }
                    insertV.add(convert(value, i));
                    deleteV.add(convert(value, i));
                    continue;
                }
                // case 2, need to insert.
                if (headers[i].startsWith(KEY_AFTER)) {
                    String column = headers[i].substring(6);
                    hasAfter = true;
                    if (!hasBefore) {
                        columns.add(column);
                    }
                    insertV.add(convert(value, i));
                    continue;
                }
                // case 3, column name is obvious.
                columns.add(headers[i]);
                if (delete) {
                    deleteV.add(convert(value, i));
                }
                insertV.add(convert(value, i));
            }
        }
        if (schemaIndex != null && tableIndex != null) {
            String schema = value.getString(schemaIndex).toString();
            String table = value.getString(tableIndex).toString();
            identityMap.put(KEY_SCHEMA, schema);
            identityMap.put(KEY_TABLE, table);
        }
    }

    /**
     * Obtain insert values and delete values from ColumnRowData according to the known column name
     *
     * @param value ColumnRowData
     * @param columns column name list
     * @param insertV insert value list
     * @param deleteV delete value list
     */
    private void wrapValuesFromRowData(
            ColumnRowData value,
            List<String> columns,
            List<String> insertV,
            List<String> deleteV,
            boolean delete) {
        String[] headers = value.getHeaders();
        if (headers == null) {
            for (String column : columns) {
                int index = columns.indexOf(column);
                insertV.add(convert(value, index));
            }
            return;
        }

        for (String column : columns) {
            for (int i = 0; i < Objects.requireNonNull(headers).length; i++) {
                if (!metaHeader.contains(headers[i])) {
                    // case 1, need to delete.
                    if (headers[i].startsWith(KEY_BEFORE)) {
                        String trueCol = headers[i].substring(7);
                        if (column.equalsIgnoreCase(trueCol)) {
                            insertV.add(convert(value, i));
                            deleteV.add(convert(value, i));
                            continue;
                        }
                    }
                    // case 2, need to insert.
                    if (headers[i].startsWith(KEY_AFTER)) {
                        String trueCol = headers[i].substring(6);
                        if (column.equalsIgnoreCase(trueCol)) {
                            insertV.add(convert(value, i));
                            continue;
                        }
                    }
                    // case 3. column name is obvious.
                    if (column.equalsIgnoreCase(headers[i])) {
                        insertV.add(convert(value, i));
                        if (delete) {
                            deleteV.add(convert(value, i));
                        }
                    }
                }
            }
        }
    }

    private Carrier initCarrier(
            List<String> columns,
            List<String> insertV,
            List<String> deleteV,
            String schema,
            String table) {
        Carrier carrier = new Carrier();
        carrier.setColumns(columns);
        carrier.setDatabase(schema);
        carrier.setTable(table);
        carrier.addInsertContent(insertV);
        carrier.addDeleteContent(deleteV);
        carrier.updateBatch();
        return carrier;
    }

    private void wrap(
            ColumnRowData value,
            List<String> columns,
            List<String> insertV,
            List<String> deleteV,
            Map<String, String> identityMap) {
        boolean delete =
                value.getRowKind() == RowKind.DELETE || value.getRowKind() == RowKind.UPDATE_BEFORE;
        /* If NameMapping is configured, RowData will carry the database
        and table names after the name matches, and the database, table,
        and column configured on the sink side are invalid.*/
        if (nameMapped) {
            wrapColumnsFromRowData(value, columns, insertV, deleteV, identityMap, delete);
        } else {
            columns.addAll(getColumnName(config.getColumn()));
            if (columns.isEmpty()) {
                // neither nameMapping nor column are set.
                wrapColumnsFromRowData(value, columns, insertV, deleteV, identityMap, delete);
            } else {
                wrapValuesFromRowData(value, columns, insertV, deleteV, delete);
                identityMap.put(KEY_SCHEMA, config.getDatabase());
                identityMap.put(KEY_TABLE, config.getTable());
            }
        }
    }

    private List<String> getColumnName(List<FieldConfig> fields) {
        List<String> columns = new LinkedList<>();
        if (fields != null) {
            fields.forEach(column -> columns.add(column.getName()));
        }
        return columns;
    }

    private String convert(@Nonnull ColumnRowData rowData, int index) {
        Object value = rowData.getField(index);
        return (value == null || "".equals(value.toString())) ? null : value.toString();
    }
}

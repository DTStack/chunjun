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

package com.dtstack.flinkx.connector.doris.rest;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.doris.options.DorisConf;
import com.dtstack.flinkx.element.ColumnRowData;

import org.apache.flink.table.data.RowData;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Company：www.dtstack.com.
 *
 * @author shitou
 * @date 2021/12/21
 */
public class DorisLoadClient implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DorisLoadClient.class);

    private final Set<String> metaHeader =
            Stream.of("schema", "table", "type", "opTime", "ts", "scn")
                    .collect(Collectors.toCollection(HashSet::new));

    private static final String LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load?";
    private static final String NULL_VALUE = "\\N";
    private static final String KEY_SCHEMA = "schema";
    private static final String KEY_TABLE = "table";
    private static final String KEY_POINT = ".";
    public static final String KEY_BEFORE = "before_";
    public static final String KEY_AFTER = "after_";

    private final DorisStreamLoad dorisStreamLoad;
    private final String fieldDelimiter;
    private final String lineDelimiter;
    private final boolean nameMapped;
    private int batchSize = 1024;
    private String hostPort;
    private final DorisConf conf;
    /** cache carriers * */
    private final Map<String, Carrier> carrierMap = new HashMap<>();

    public DorisLoadClient(DorisStreamLoad dorisStreamLoad, DorisConf conf, String hostPort) {
        this.dorisStreamLoad = dorisStreamLoad;
        this.hostPort = hostPort;
        this.conf = conf;
        this.nameMapped = conf.isNameMapped();
        this.batchSize = conf.getBatchSize();
        this.fieldDelimiter = conf.getFieldDelimiter();
        this.lineDelimiter = conf.getLineDelimiter();
    }

    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
    }

    public void load(RowData value, boolean single) throws IOException {
        String schema;
        String table;
        List<String> columns = new LinkedList<>();
        List<String> insertV = new LinkedList<>();
        List<String> deleteV = new LinkedList<>();
        // sync job.
        if (value instanceof ColumnRowData) {
            /* If NameMapping is configured, RowData will carry the database
            and table names after the name matches, and the database, table,
            and column configured on the sink side are invalid.*/
            if (nameMapped) {
                Map<String, String> identityMap = new HashMap<>(2);
                wrapColumnsFromRowData(
                        (ColumnRowData) value, columns, insertV, deleteV, identityMap);
                schema = MapUtils.getString(identityMap, KEY_SCHEMA, conf.getDatabase());
                table = MapUtils.getString(identityMap, KEY_TABLE, conf.getTable());
            } else {
                columns = getColumnName(conf.getColumn());
                if (columns.isEmpty()) {
                    // neither nameMapping nor column are set.
                    Map<String, String> identityMap = new HashMap<>(2);
                    wrapColumnsFromRowData(
                            (ColumnRowData) value, columns, insertV, deleteV, identityMap);
                    schema = MapUtils.getString(identityMap, KEY_SCHEMA, conf.getDatabase());
                    table = MapUtils.getString(identityMap, KEY_TABLE, conf.getTable());
                } else {
                    wrapValuesFromRowData((ColumnRowData) value, columns, insertV, deleteV);
                    schema = conf.getDatabase();
                    table = conf.getTable();
                }
            }
            loadData(columns, insertV, deleteV, schema, table, single);
        }
        // TODO sql support
    }

    private void loadData(
            List<String> columns,
            List<String> insertV,
            List<String> deleteV,
            String schema,
            String table,
            boolean single)
            throws IOException {
        // if batchSize is one or write SingleRecord.
        if (single) {
            singleLoad(columns, insertV, deleteV, schema, table);
            return;
        }
        String key = schema + KEY_POINT + table;
        if (carrierMap.containsKey(key)) {
            Carrier carrier = carrierMap.get(key);
            carrier.addInsertContent(insertV);
            carrier.addDeleteContent(deleteV);
            carrier.updateBatch();
            if (carrier.isFull()) {
                dorisStreamLoad.load(
                        carrier, String.format(LOAD_URL_PATTERN, hostPort, schema, table));
                carrierMap.remove(key);
            }

        } else {
            Carrier carrier = new Carrier(fieldDelimiter, lineDelimiter, batchSize);
            carrier.setColumns(columns);
            carrier.addInsertContent(insertV);
            carrier.addDeleteContent(deleteV);
            carrier.updateBatch();
            carrierMap.put(key, carrier);
        }
    }

    public String getHostPort() {
        return hostPort;
    }

    private void singleLoad(
            List<String> columns,
            List<String> insertV,
            List<String> deleteV,
            String schema,
            String table)
            throws IOException {
        Carrier carrier = new Carrier(fieldDelimiter, lineDelimiter, batchSize);
        carrier.setColumns(columns);
        carrier.addInsertContent(insertV);
        carrier.addDeleteContent(deleteV);
        dorisStreamLoad.load(carrier, String.format(LOAD_URL_PATTERN, hostPort, schema, table));
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
            Map<String, String> identityMap) {

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
                // case 3, simple insert.
                columns.add(headers[i]);
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
            ColumnRowData value, List<String> columns, List<String> insertV, List<String> deleteV) {
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
                            break;
                        }
                    }
                    // case 2, need to insert.
                    if (headers[i].startsWith(KEY_AFTER)) {
                        String trueCol = headers[i].substring(6);
                        if (column.equalsIgnoreCase(trueCol)) {
                            insertV.add(convert(value, i));
                            break;
                        }
                    }
                    // case 3. simple insert.
                    if (column.equalsIgnoreCase(headers[i])) {
                        insertV.add(convert(value, i));
                        break;
                    }
                }
            }
        }
    }

    private List<String> getColumnName(List<FieldConf> fields) {
        List<String> columns = new LinkedList<>();
        if (fields != null) {
            fields.forEach(column -> columns.add(column.getName()));
        }
        return columns;
    }

    private String convert(@Nonnull ColumnRowData rowData, int index) {
        Object value = rowData.getField(index);
        return (value == null || "".equals(value.toString())) ? NULL_VALUE : value.toString();
    }
}

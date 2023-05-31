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

package com.dtstack.chunjun.connector.kudu.util;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.kudu.config.KuduCommonConfig;
import com.dtstack.chunjun.connector.kudu.config.KuduSourceConfig;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.security.KerberosConfig;
import com.dtstack.chunjun.security.KerberosUtil;
import com.dtstack.chunjun.throwable.NoRestartException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;

import java.io.IOException;
import java.math.BigDecimal;
import java.security.PrivilegedExceptionAction;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KuduUtil {
    private static final String EXPRESS_REGEX =
            "(?<column>[^\\=|\\s]+)+\\s*(?<op>[\\>|\\<|\\=]+)\\s*(?<value>.*)";

    private static final String FILTER_SPLIT_REGEX = "(?i)\\s+and\\s+";

    private static final Pattern EXPRESS_PATTERN = Pattern.compile(EXPRESS_REGEX);

    /**
     * Get kudu client with kudu conf
     *
     * @param config kudu conf
     * @return kudu sync client
     */
    public static KuduClient getKuduClient(KuduCommonConfig config) {
        try {
            KerberosConfig kerberosConfig = config.getKerberos();
            if (kerberosConfig != null && kerberosConfig.isEnableKrb()) {

                UserGroupInformation ugi = KerberosUtil.loginAndReturnUgi(kerberosConfig);
                return ugi.doAs(
                        (PrivilegedExceptionAction<KuduClient>)
                                () -> getKuduClientInternal(config));
            } else {
                return getKuduClientInternal(config);
            }
        } catch (IOException | InterruptedException e) {
            throw new NoRestartException("Create kudu client failed!", e);
        }
    }

    public static AsyncKuduClient getAsyncKuduClient(KuduCommonConfig config) {
        try {
            KerberosConfig kerberosConfig = config.getKerberos();
            if (kerberosConfig.isEnableKrb()) {
                UserGroupInformation ugi = KerberosUtil.loginAndReturnUgi(kerberosConfig);
                return ugi.doAs(
                        (PrivilegedExceptionAction<AsyncKuduClient>)
                                () -> getAsyncKuduClientInternal(config));
            } else {
                return getAsyncKuduClientInternal(config);
            }
        } catch (IOException | InterruptedException e) {
            throw new NoRestartException("Create kudu client failed!", e);
        }
    }

    private static AsyncKuduClient getAsyncKuduClientInternal(KuduCommonConfig config) {
        return new AsyncKuduClient.AsyncKuduClientBuilder(
                        Arrays.asList(config.getMasters().split(",")))
                .workerCount(config.getWorkerCount())
                .defaultAdminOperationTimeoutMs(config.getAdminOperationTimeout())
                .defaultOperationTimeoutMs(config.getOperationTimeout())
                .build();
    }

    private static KuduClient getKuduClientInternal(KuduCommonConfig config) {
        return new AsyncKuduClient.AsyncKuduClientBuilder(
                        Arrays.asList(config.getMasters().split(",")))
                .workerCount(config.getWorkerCount())
                .defaultAdminOperationTimeoutMs(config.getAdminOperationTimeout())
                .defaultOperationTimeoutMs(config.getOperationTimeout())
                .build()
                .syncClient();
    }

    public static List<KuduScanToken> getKuduScanToken(KuduSourceConfig config) throws IOException {
        try (KuduClient client = KuduUtil.getKuduClient(config)) {
            KuduTable kuduTable = client.openTable(config.getTable());

            List<String> columnNameList = new ArrayList<>();

            List<FieldConfig> columnList = config.getColumn();
            columnList.forEach(item -> columnNameList.add(item.getName()));

            KuduScanToken.KuduScanTokenBuilder builder =
                    client.newScanTokenBuilder(kuduTable)
                            .readMode(getReadMode(config.getReadMode()))
                            .batchSizeBytes(config.getBatchSizeBytes())
                            .setTimeout(config.getQueryTimeout())
                            .setProjectedColumnNames(columnNameList);

            // 添加过滤条件
            addPredicates(builder, config.getFilter(), columnList);

            return builder.build();
        } catch (Exception e) {
            throw new IOException("Get ScanToken error", e);
        }
    }

    private static AsyncKuduScanner.ReadMode getReadMode(String readMode) {
        if (AsyncKuduScanner.ReadMode.READ_LATEST.name().equalsIgnoreCase(readMode)) {
            return AsyncKuduScanner.ReadMode.READ_LATEST;
        } else {
            return AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT;
        }
    }

    private static void addPredicates(
            KuduScanToken.KuduScanTokenBuilder builder,
            String filterString,
            List<FieldConfig> columns) {
        if (StringUtils.isEmpty(filterString)) {
            return;
        }

        Map<String, Type> nameTypeMap = new HashMap<>(columns.size());

        columns.forEach(
                item -> {
                    String name = item.getName();
                    String type = item.getType().getType();
                    nameTypeMap.put(name, getType(type));
                });

        String[] filters = filterString.split(FILTER_SPLIT_REGEX);
        for (String filter : filters) {
            if (StringUtils.isNotBlank(filter)) {
                ExpressResult expressResult = parseExpress(filter, nameTypeMap);
                KuduPredicate predicate =
                        KuduPredicate.newComparisonPredicate(
                                expressResult.getColumnSchema(),
                                expressResult.getOperator(),
                                expressResult.getValue());
                builder.addPredicate(predicate);
            }
        }
    }

    public static Type getType(String columnType) {
        switch (columnType.toLowerCase()) {
            case "boolean":
            case "bool":
                return Type.BOOL;
            case "int8":
            case "byte":
                return Type.INT8;
            case "int16":
            case "short":
                return Type.INT16;
            case "int32":
            case "integer":
            case "int":
                return Type.INT32;
            case "int64":
            case "bigint":
            case "long":
                return Type.INT64;
            case "float":
                return Type.FLOAT;
            case "double":
                return Type.DOUBLE;
            case "decimal":
                return Type.DECIMAL;
            case "binary":
                return Type.BINARY;
            case "char":
            case "varchar":
            case "text":
            case "string":
                return Type.STRING;
            case "unixtime_micros":
            case "timestamp":
                return Type.UNIXTIME_MICROS;
            default:
                throw new IllegalArgumentException("Not support column type:" + columnType);
        }
    }

    public static ExpressResult parseExpress(String express, Map<String, Type> nameTypeMap) {
        Matcher matcher = EXPRESS_PATTERN.matcher(express.trim());
        if (matcher.find()) {
            String column = matcher.group("column");
            String op = matcher.group("op");
            String value = matcher.group("value");

            Type type = nameTypeMap.get(column.trim());
            if (type == null) {
                throw new IllegalArgumentException(
                        "Can not find column:" + column + " from column list");
            }

            ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder(column, type).build();

            ExpressResult result = new ExpressResult();
            result.setColumnSchema(columnSchema);
            result.setOperator(getOp(op));
            result.setValue(getValue(value, type));

            return result;
        } else {
            throw new IllegalArgumentException("Illegal filter express:" + express);
        }
    }

    public static Object getValue(String value, Type type) {
        if (value == null) {
            return null;
        }

        if (value.startsWith(ConstantValue.DOUBLE_QUOTE_MARK_SYMBOL)
                || value.endsWith(ConstantValue.SINGLE_QUOTE_MARK_SYMBOL)) {
            value = value.substring(1, value.length() - 1);
        }

        Object objValue;
        if (Type.BOOL.equals(type)) {
            objValue = Boolean.valueOf(value);
        } else if (Type.INT8.equals(type)) {
            objValue = Byte.valueOf(value);
        } else if (Type.INT16.equals(type)) {
            objValue = Short.valueOf(value);
        } else if (Type.INT32.equals(type)) {
            objValue = Integer.valueOf(value);
        } else if (Type.INT64.equals(type)) {
            objValue = Long.valueOf(value);
        } else if (Type.FLOAT.equals(type)) {
            objValue = Float.valueOf(value);
        } else if (Type.DOUBLE.equals(type)) {
            objValue = Double.valueOf(value);
        } else if (Type.DECIMAL.equals(type)) {
            objValue = new BigDecimal(value);
        } else if (Type.UNIXTIME_MICROS.equals(type)) {
            if (NumberUtils.isNumber(value)) {
                objValue = Long.valueOf(value);
            } else {
                objValue = Timestamp.valueOf(value);
            }
        } else {
            objValue = value;
        }

        return objValue;
    }

    private static KuduPredicate.ComparisonOp getOp(String opExpress) {
        switch (opExpress) {
            case "=":
                return KuduPredicate.ComparisonOp.EQUAL;
            case ">":
                return KuduPredicate.ComparisonOp.GREATER;
            case ">=":
                return KuduPredicate.ComparisonOp.GREATER_EQUAL;
            case "<":
                return KuduPredicate.ComparisonOp.LESS;
            case "<=":
                return KuduPredicate.ComparisonOp.LESS_EQUAL;
            default:
                throw new IllegalArgumentException(
                        "Comparison express only support '=','>','>=','<','<='");
        }
    }

    public static class ExpressResult {
        private ColumnSchema columnSchema;
        private KuduPredicate.ComparisonOp op;
        private Object value;

        public ColumnSchema getColumnSchema() {
            return columnSchema;
        }

        public void setColumnSchema(ColumnSchema columnSchema) {
            this.columnSchema = columnSchema;
        }

        public KuduPredicate.ComparisonOp getOperator() {
            return op;
        }

        public void setOperator(KuduPredicate.ComparisonOp op) {
            this.op = op;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }
    }

    public static void setMapValue(
            Type type, Map<String, Object> oneRow, String sideFieldName, RowResult result) {
        switch (type) {
            case STRING:
                oneRow.put(sideFieldName, result.getString(sideFieldName));
                break;
            case FLOAT:
                oneRow.put(sideFieldName, result.getFloat(sideFieldName));
                break;
            case INT8:
                oneRow.put(sideFieldName, result.getByte(sideFieldName));
                break;
            case INT16:
                oneRow.put(sideFieldName, result.getShort(sideFieldName));
                break;
            case INT32:
                oneRow.put(sideFieldName, result.getInt(sideFieldName));
                break;
            case INT64:
                oneRow.put(sideFieldName, result.getLong(sideFieldName));
                break;
            case DOUBLE:
                oneRow.put(sideFieldName, result.getDouble(sideFieldName));
                break;
            case BOOL:
                oneRow.put(sideFieldName, result.getBoolean(sideFieldName));
                break;
            case UNIXTIME_MICROS:
                oneRow.put(sideFieldName, result.getTimestamp(sideFieldName));
                break;
            case BINARY:
                oneRow.put(sideFieldName, result.getBinary(sideFieldName));
                break;
            default:
                throw new IllegalArgumentException("Illegal var type: " + type);
        }
    }
}

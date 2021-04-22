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


package com.dtstack.flinkx.kudu.core;

import com.dtstack.flinkx.authenticate.KerberosUtil;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.FileSystemUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduTable;

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

/**
 * @author jiangbo
 * @date 2019/7/31
 */
public class KuduUtil {

    private static String FILTER_SPLIT_REGEX = "(?i)\\s+and\\s+";
    private static String EXPRESS_REGEX = "(?<column>[^\\=|\\s]+)+\\s*(?<op>[\\>|\\<|\\=]+)\\s*(?<value>.*)";
    private static Pattern EXPRESS_PATTERN = Pattern.compile(EXPRESS_REGEX);

    public final static String AUTHENTICATION_TYPE = "Kerberos";

    /**
     * 获取kudu的客户端
     * @param config  kudu的配置信息
     * @param hadoopConfig hadoop相关信息 主要需要kerberos相关验证信息
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public static KuduClient getKuduClient(KuduConfig config, Map<String,Object> hadoopConfig) throws IOException,InterruptedException {
        if(AUTHENTICATION_TYPE.equals(config.getAuthentication()) && FileSystemUtil.isOpenKerberos(hadoopConfig)){
            UserGroupInformation ugi = FileSystemUtil.getUGI(hadoopConfig,null);
            return ugi.doAs(new PrivilegedExceptionAction<KuduClient>() {
                @Override
                public KuduClient run() throws Exception {
                    return getKuduClientInternal(config);
                }
            });
        } else {
            return getKuduClientInternal(config);
        }
    }

    private static KuduClient getKuduClientInternal(KuduConfig config) {
        return new AsyncKuduClient.AsyncKuduClientBuilder(Arrays.asList(config.getMasterAddresses().split(",")))
                .workerCount(config.getWorkerCount())
                .bossCount(config.getBossCount())
                .defaultAdminOperationTimeoutMs(config.getAdminOperationTimeout())
                .defaultOperationTimeoutMs(config.getOperationTimeout())
                .build()
                .syncClient();
    }

    public static List<KuduScanToken> getKuduScanToken(KuduConfig config, List<MetaColumn> columns, String filterString, Map<String,Object> hadoopConfig) throws IOException{
        try (
                KuduClient client = getKuduClient(config, hadoopConfig)
        ) {
            KuduTable kuduTable = client.openTable(config.getTable());

            List<String> columnNames = new ArrayList<>(columns.size());
            for (MetaColumn column : columns) {
               if(column.getValue() == null){
                   columnNames.add(column.getName());
               }
            }

            KuduScanToken.KuduScanTokenBuilder builder = client.newScanTokenBuilder(kuduTable)
                    .readMode(getReadMode(config.getReadMode()))
                    .batchSizeBytes(config.getBatchSizeBytes())
                    .setTimeout(config.getQueryTimeout())
                    .setProjectedColumnNames(columnNames);

            //添加过滤条件
            addPredicates(builder, filterString, columns);

            return builder.build();
        } catch (Exception e) {
            throw new IOException("Get ScanToken error", e);
        }
    }

    private static AsyncKuduScanner.ReadMode getReadMode(String readMode){
        if(AsyncKuduScanner.ReadMode.READ_LATEST.name().equalsIgnoreCase(readMode)){
            return AsyncKuduScanner.ReadMode.READ_LATEST;
        } else {
            return AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT;
        }
    }

    private static void addPredicates(KuduScanToken.KuduScanTokenBuilder builder, String filterString, List<MetaColumn> columns){
        if(StringUtils.isEmpty(filterString)){
            return;
        }

        Map<String, Type> nameTypeMap = new HashMap<>(columns.size());
        for (MetaColumn column : columns) {
            nameTypeMap.put(column.getName(), getType(column.getType()));
        }

        String[] filters = filterString.split(FILTER_SPLIT_REGEX);
        for (String filter : filters) {
            if(StringUtils.isNotBlank(filter)){
                ExpressResult expressResult = parseExpress(filter, nameTypeMap);
                KuduPredicate predicate = KuduPredicate.newComparisonPredicate(expressResult.getColumnSchema(), expressResult.getOp(), expressResult.getValue());
                builder.addPredicate(predicate);
            }
        }
    }

    public static Type getType(String columnType){
        switch (columnType.toLowerCase()){
            case "boolean" :
            case "bool" : return Type.BOOL;
            case "int8":
            case "byte" : return  Type.INT8;
            case "int16":
            case "short" : return  Type.INT16;
            case "int32":
            case "integer":
            case "int" : return  Type.INT32;
            case "int64":
            case "bigint":
            case "long" : return  Type.INT64;
            case "float" : return  Type.FLOAT;
            case "double" : return  Type.DOUBLE;
            case "decimal" : return  Type.DECIMAL;
            case "binary" : return Type.BINARY;
            case "char":
            case "varchar":
            case "text":
            case "string" : return  Type.STRING;
            case "unixtime_micros":
            case "timestamp" : return  Type.UNIXTIME_MICROS;
            default:
                throw new IllegalArgumentException("Not support column type:" + columnType);
        }
    }

    public static ExpressResult parseExpress(String express, Map<String, Type> nameTypeMap){
        Matcher matcher = EXPRESS_PATTERN.matcher(express.trim());
        if (matcher.find()) {
            String column = matcher.group("column");
            String op = matcher.group("op");
            String value = matcher.group("value");

            Type type = nameTypeMap.get(column.trim());
            if(type == null){
                throw new IllegalArgumentException("Can not find column:" + column + " from column list");
            }

            ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder(column, type).build();

            ExpressResult result = new ExpressResult();
            result.setColumnSchema(columnSchema);
            result.setOp(getOp(op));
            result.setValue(getValue(value, type));

            return result;
        } else {
            throw new IllegalArgumentException("Illegal filter express:" + express);
        }
    }

    public static Object getValue(String value, Type type){
        if(value == null){
            return null;
        }

        if(value.startsWith(ConstantValue.DOUBLE_QUOTE_MARK_SYMBOL) || value.endsWith(ConstantValue.SINGLE_QUOTE_MARK_SYMBOL)){
            value = value.substring(1, value.length() - 1);
        }

        Object objValue;
        if (Type.BOOL.equals(type)){
            objValue = Boolean.valueOf(value);
        } else if(Type.INT8.equals(type)){
            objValue = Byte.valueOf(value);
        } else if(Type.INT16.equals(type)){
            objValue = Short.valueOf(value);
        } else if(Type.INT32.equals(type)){
            objValue = Integer.valueOf(value);
        } else if(Type.INT64.equals(type)){
            objValue = Long.valueOf(value);
        } else if(Type.FLOAT.equals(type)){
            objValue = Float.valueOf(value);
        } else if(Type.DOUBLE.equals(type)){
            objValue = Double.valueOf(value);
        } else if(Type.DECIMAL.equals(type)){
            objValue = new BigDecimal(value);
        } else if(Type.UNIXTIME_MICROS.equals(type)){
            if(NumberUtils.isNumber(value)){
                objValue = Long.valueOf(value);
            } else {
                objValue = Timestamp.valueOf(value);
            }
        } else {
            objValue = value;
        }

        return objValue;
    }

    private static KuduPredicate.ComparisonOp getOp(String opExpress){
        switch (opExpress){
            case "=" : return KuduPredicate.ComparisonOp.EQUAL;
            case ">" : return KuduPredicate.ComparisonOp.GREATER;
            case ">=" : return KuduPredicate.ComparisonOp.GREATER_EQUAL;
            case "<" : return KuduPredicate.ComparisonOp.LESS;
            case "<=" : return KuduPredicate.ComparisonOp.LESS_EQUAL;
            default:
                throw new IllegalArgumentException("Comparison express only support '=','>','>=','<','<='");
        }
    }

    public static class ExpressResult{
        private ColumnSchema columnSchema;
        private KuduPredicate.ComparisonOp op;
        private Object value;

        public ColumnSchema getColumnSchema() {
            return columnSchema;
        }

        public void setColumnSchema(ColumnSchema columnSchema) {
            this.columnSchema = columnSchema;
        }

        public KuduPredicate.ComparisonOp getOp() {
            return op;
        }

        public void setOp(KuduPredicate.ComparisonOp op) {
            this.op = op;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }
    }
}

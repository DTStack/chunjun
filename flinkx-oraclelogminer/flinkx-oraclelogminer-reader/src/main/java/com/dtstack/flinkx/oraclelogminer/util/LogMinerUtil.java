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


package com.dtstack.flinkx.oraclelogminer.util;

import com.dtstack.flinkx.oraclelogminer.format.LogMinerConfig;
import com.dtstack.flinkx.util.SnowflakeIdWorker;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.types.Row;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

/**
 * @author jiangbo
 * @date 2019/12/14
 */
public class LogMinerUtil {

    public final static String KEY_SEG_OWNER = "SEG_OWNER";
    public final static String KEY_TABLE_NAME = "TABLE_NAME";
    public final static String KEY_OPERATION = "OPERATION";
    public final static String KEY_TIMESTAMP = "TIMESTAMP";
    public final static String KEY_SQL_REDO = "SQL_REDO";
    public final static String KEY_CSF = "CSF";
    public final static String KEY_SCN = "SCN";
    public final static String KEY_CURRENT_SCN = "CURRENT_SCN";
    public final static String KEY_FIRST_CHANGE = "FIRST_CHANGE#";

    /**
     * 启动logminer
     * 视图说明：
     * v$log：存储未归档的日志
     * v$archived_log：存储已归档的日志文件
     * v$logfile：
     */
    public final static String SQL_START_LOGMINER = "" +
            "DECLARE\n" +
            "    st          BOOLEAN := true;\n" +
            "    start_scn   NUMBER := ?;\n" +
            "BEGIN\n" +
            "    FOR l_log_rec IN (\n" +
            "        SELECT\n" +
            "            MIN(name) name,\n" +
            "            first_change#,\n" +
            "            next_change#\n" +
            "        FROM\n" +
            "            (\n" +
            "                SELECT\n" +
            "                    MIN(member) AS name,\n" +
            "                    first_change#,\n" +
            "                    next_change#\n" +
            "                FROM\n" +
            "                    v$log       l\n" +
            "                    INNER JOIN v$logfile   f ON l.group# = f.group#\n" +
            "                GROUP BY\n" +
            "                    first_change#,\n" +
            "                    next_change#\n" +
            "                UNION\n" +
            "                SELECT\n" +
            "                    name,\n" +
            "                    first_change#,\n" +
            "                    next_change#\n" +
            "                FROM\n" +
            "                    v$archived_log\n" +
            "                WHERE\n" +
            "                    name IS NOT NULL\n" +
            "            )\n" +
            "        WHERE\n" +
            "            first_change# >= start_scn\n" +
            "            OR start_scn < next_change#\n" +
            "        GROUP BY\n" +
            "            first_change#,\n" +
            "            next_change#\n" +
            "        ORDER BY\n" +
            "            first_change#\n" +
            "    ) LOOP IF st THEN\n" +
            "        dbms_logmnr.add_logfile(l_log_rec.name, dbms_logmnr.new);\n" +
            "        st := false;\n" +
            "    ELSE\n" +
            "        dbms_logmnr.add_logfile(l_log_rec.name);\n" +
            "    END IF;\n" +
            "    END LOOP;\n" +
            "\n" +
            "    dbms_logmnr.start_logmnr(options => " +
            "dbms_logmnr.skip_corruption " +
            "+ dbms_logmnr.no_sql_delimiter " +
            "+ dbms_logmnr.no_rowid_in_stmt\n" +
            "+ dbms_logmnr.dict_from_online_catalog " +
            "+ DBMS_LOGMNR.committed_data_only" +
            "+ dbms_logmnr.string_literals_in_stmt);\n" +
            "\n" +
            "END;";

    public final static String SQL_SELECT_DATA = "" +
            "SELECT\n" +
            "    scn,\n" +
            "    commit_scn,\n" +
            "    timestamp,\n" +
            "    operation,\n" +
            "    seg_owner,\n" +
            "    table_name,\n" +
            "    sql_redo,\n" +
            "    row_id,\n" +
            "    csf\n" +
            "FROM\n" +
            "    v$logmnr_contents\n" +
            "WHERE\n" +
            "    scn >= ?";

    public final static String SQL_GET_CURRENT_SCN = "select min(CURRENT_SCN) CURRENT_SCN from gv$database";

    public final static String SQL_GET_LOG_FILE_START_POSITION = "select min(FIRST_CHANGE#) FIRST_CHANGE# from (select FIRST_CHANGE# from v$log union select FIRST_CHANGE# from v$archived_log where standby_dest='NO')";

    public final static String SQL_GET_LOG_FILE_START_POSITION_BY_SCN = "select min(FIRST_CHANGE#) FIRST_CHANGE# from (select FIRST_CHANGE# from v$log where ? between FIRST_CHANGE# and NEXT_CHANGE# union select FIRST_CHANGE# from v$archived_log where ? between FIRST_CHANGE# and NEXT_CHANGE# and standby_dest='NO')";

    public final static String SQL_GET_LOG_FILE_START_POSITION_BY_TIME = "select min(FIRST_CHANGE#) FIRST_CHANGE# from (select FIRST_CHANGE# from v$log where TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS') between FIRST_TIME and NVL(NEXT_TIME, TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')) union select FIRST_CHANGE# from v$archived_log where TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS') between FIRST_TIME and NEXT_TIME and standby_dest='NO')";

    private final static List<String> SUPPORTED_OPERATIONS = Arrays.asList("UPDATE", "INSERT", "DELETE");

    public static List<String> EXCLUDE_SCHEMAS = Arrays.asList("SYS");

    public static SnowflakeIdWorker idWorker = new SnowflakeIdWorker(1, 1);

    public static String buildSelectSql(String listenerOptions, String listenerTables){
        StringBuilder sqlBuilder = new StringBuilder(SQL_SELECT_DATA);

        if (StringUtils.isNotEmpty(listenerOptions)) {
            sqlBuilder.append(" and ").append(buildOperationFilter(listenerOptions));
        }

        if (StringUtils.isNotEmpty(listenerTables)) {
            sqlBuilder.append(" and ").append(buildSchemaTableFilter(listenerTables));
        } else {
            sqlBuilder.append(" and ").append(buildExcludeSchemaFilter());
        }

        return sqlBuilder.toString();
    }

    private static String buildOperationFilter(String listenerOptions){
        List<String> standardOperations = new ArrayList<>();

        String[] operations = listenerOptions.split(",");
        for (String operation : operations) {
            if (!SUPPORTED_OPERATIONS.contains(operation.toUpperCase())) {
                throw new RuntimeException("不支持的操作类型:" + operation);
            }

            standardOperations.add(String.format("'%s'", operation.toUpperCase()));
        }

        return String.format("OPERATION in (%s) ", StringUtils.join(standardOperations, ","));
    }

    private static String buildExcludeSchemaFilter(){
        List<String> filters = new ArrayList<>();
        for (String excludeSchema : EXCLUDE_SCHEMAS) {
            filters.add(String.format("SEG_OWNER != '%s'", excludeSchema));
        }

        return String.format("(%s)", StringUtils.join(filters, " and "));
    }

    private static String buildSchemaTableFilter(String listenerTables){
        List<String> filters = new ArrayList<>();

        String[] tableWithSchemas = listenerTables.split(",");
        for (String tableWithSchema : tableWithSchemas){
            List<String> tables = Arrays.asList(tableWithSchema.split("\\."));
            if ("*".equals(tables.get(0))) {
                throw new IllegalArgumentException("必须指定要采集的schema:" + tableWithSchema);
            }

            StringBuilder tableFilterBuilder = new StringBuilder();
            tableFilterBuilder.append(String.format("SEG_OWNER='%s'", tables.get(0)));

            if(!"*".equals(tables.get(1))){
                tableFilterBuilder.append(" and ").append(String.format("TABLE_NAME='%s'", tables.get(1)));
            }

            filters.add(String.format("(%s)", tableFilterBuilder.toString()));
        }

        return String.format("(%s)", StringUtils.join(filters, " or "));
    }

    public static Row parseSql(ResultSet logMinerData, String sqlRedo, boolean pavingData) throws JSQLParserException, SQLException {
        String schema = logMinerData.getString(KEY_SEG_OWNER);
        String tableName = logMinerData.getString(KEY_TABLE_NAME);
        String operation = logMinerData.getString(KEY_OPERATION);
        Timestamp timestamp = logMinerData.getTimestamp(KEY_TIMESTAMP);

        final Map<String,Object> message = new LinkedHashMap<>();
        message.put("type", operation);
        message.put("schema", schema);
        message.put("table", tableName);
        message.put("ts", idWorker.nextId());

        String sqlRedo2=sqlRedo.replace("IS NULL", "= NULL");
        Statement stmt = CCJSqlParserUtil.parse(sqlRedo2);
        LinkedHashMap<String,String> afterDataMap = new LinkedHashMap<>();
        LinkedHashMap<String,String> beforeDataMap = new LinkedHashMap<>();

        if (stmt instanceof Insert){
            parseInsertStmt((Insert) stmt, beforeDataMap, afterDataMap);
        }else if (stmt instanceof Update){
            parseUpdateStmt((Update) stmt, beforeDataMap, afterDataMap);
        }else if (stmt instanceof Delete){
            parseDeleteStmt((Delete) stmt, beforeDataMap, afterDataMap);
        }

        if (pavingData) {
            afterDataMap.forEach((key, val) -> {
                message.put("after_" + key, val);
            });

            beforeDataMap.forEach((key, val) -> {
                message.put("before_" + key, val);
            });

            return Row.of(message);
        } else {
            message.put("before", beforeDataMap);
            message.put("after", afterDataMap);
            Map<String,Object> event = new HashMap<>(1);
            event.put("message", message);

            return Row.of(event);
        }
    }

    private static void parseInsertStmt(Insert insert, LinkedHashMap<String,String> beforeDataMap, LinkedHashMap<String,String> afterDataMap){
        for (Column column : insert.getColumns()){
            afterDataMap.put(cleanString(column.getColumnName()), null);
        }

        ExpressionList eList = (ExpressionList) insert.getItemsList();
        List<Expression> valueList = eList.getExpressions();
        int i =0;
        for (String key : afterDataMap.keySet()){
            String value = cleanString(valueList.get(i).toString());
            afterDataMap.put(key, value);
            beforeDataMap.put(key, null);
            i++;
        }
    }

    private static void parseUpdateStmt(Update update, LinkedHashMap<String,String> beforeDataMap, LinkedHashMap<String,String> afterDataMap){
        for (Column c : update.getColumns()){
            afterDataMap.put(cleanString(c.getColumnName()), null);
        }

        Iterator<Expression> iterator = update.getExpressions().iterator();

        for (String key : afterDataMap.keySet()){
            Object o = iterator.next();
            String value = cleanString(o.toString());
            afterDataMap.put(key, value);
        }

        update.getWhere().accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(final EqualsTo expr){
                String col = cleanString(expr.getLeftExpression().toString());
                if(afterDataMap.containsKey(col)){
                    String value = cleanString(expr.getRightExpression().toString());
                    beforeDataMap.put(col, value);
                } else {
                    String value = cleanString(expr.getRightExpression().toString());
                    beforeDataMap.put(col, value);
                    afterDataMap.put(col, value);
                }
            }
        });
    }

    private static void parseDeleteStmt(Delete delete, LinkedHashMap<String,String> beforeDataMap, LinkedHashMap<String,String> afterDataMap){
        delete.getWhere().accept(new ExpressionVisitorAdapter(){
            @Override
            public void visit(final EqualsTo expr){
                String col = cleanString(expr.getLeftExpression().toString());
                String value = cleanString(expr.getRightExpression().toString());
                beforeDataMap.put(col, value);
                afterDataMap.put(col, null);
            }
        });
    }

    private static String cleanString(String str) {
        if (str.startsWith("TIMESTAMP")) {
            str = str.replace("TIMESTAMP ", "");
        }

        if (str.startsWith("'") && str.endsWith("'")) {
            str = str.substring(1, str.length() - 1);
        }

        if (str.startsWith("\"") && str.endsWith("\"")) {
            str = str.substring(1, str.length() - 1);
        }

        return str.replace("IS NULL","= NULL").trim();
    }

    public static boolean isCreateTemporaryTableSql(String sql) {
        return sql.contains("temporary tables");
    }

    public static void configStatement(java.sql.Statement statement, LogMinerConfig config) throws SQLException{
        if (config.getQueryTimeout() != null) {
            statement.setQueryTimeout(config.getQueryTimeout().intValue());
        }
    }
}
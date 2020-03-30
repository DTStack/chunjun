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

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author jiangbo
 * @date 2019/12/14
 */
public class SqlUtil {

    /**
     * OPTIONS参数说明:
     * DBMS_LOGMNR.SKIP_CORRUPTION - 跳过出错的redlog
     * DBMS_LOGMNR.NO_SQL_DELIMITER - 不使用 ';'分割redo sql
     * DBMS_LOGMNR.NO_ROWID_IN_STMT - 默认情况下，用于UPDATE和DELETE操作的SQL_REDO和SQL_UNDO语句在where子句中包含“ ROWID =”。
     *                                但是，这对于想要重新执行SQL语句的应用程序是不方便的。设置此选项后，“ ROWID”不会放置在重构语句的末尾
     * DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG - 使用在线字典
     * DBMS_LOGMNR.CONTINUOUS_MINE - 需要在生成重做日志的同一实例中使用日志
     * DBMS_LOGMNR.COMMITTED_DATA_ONLY - 指定此选项时，LogMiner将属于同一事务的所有DML操作分组在一起。事务按提交顺序返回。
     * DBMS_LOGMNR.STRING_LITERALS_IN_STMT - 默认情况下，格式化格式化的SQL语句时，SQL_REDO和SQL_UNDO语句会使用数据库会话的NLS设置
     *                                       例如NLS_DATE_FORMAT，NLS_NUMERIC_CHARACTERS等）。使用此选项，将使用ANSI / ISO字符串文字格式对重构的SQL语句进行格式化。
     */
    public final static String SQL_START_LOG_MINER_AUTO_ADD_LOG = "" +
            "BEGIN DBMS_LOGMNR.START_LOGMNR(" +
            "STARTSCN => ?," +
            "OPTIONS => DBMS_LOGMNR.SKIP_CORRUPTION " +
            "+ DBMS_LOGMNR.NO_SQL_DELIMITER " +
            "+ DBMS_LOGMNR.NO_ROWID_IN_STMT " +
            "+ DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG " +
            "+ DBMS_LOGMNR.CONTINUOUS_MINE " +
            "+ DBMS_LOGMNR.COMMITTED_DATA_ONLY " +
            "+ DBMS_LOGMNR.STRING_LITERALS_IN_STMT" +
            ");" +
            "END;";

    /**
     * 启动logminer
     * 视图说明：
     * v$log：存储未归档的日志
     * v$archived_log：存储已归档的日志文件
     * v$logfile：
     */
    public final static String SQL_START_LOG_MINER = "" +
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
            "        dbms_logmnr.add_logfile(l_log_rec.name, dbms_logmnr.addfile);\n" +
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

    public static boolean isCreateTemporaryTableSql(String sql) {
        return sql.contains("temporary tables");
    }
}
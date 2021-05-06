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

import com.dtstack.flinkx.constants.ConstantValue;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
            "BEGIN SYS.DBMS_LOGMNR.START_LOGMNR(" +
            "   STARTSCN => ?," +
            "   OPTIONS => SYS.DBMS_LOGMNR.SKIP_CORRUPTION " +
            "       + SYS.DBMS_LOGMNR.NO_SQL_DELIMITER " +
            "       + SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT " +
            "       + SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG " +
            "       + SYS.DBMS_LOGMNR.CONTINUOUS_MINE " +
            "       + SYS.DBMS_LOGMNR.COMMITTED_DATA_ONLY " +
            "       + SYS.DBMS_LOGMNR.STRING_LITERALS_IN_STMT" +
            ");" +
            "END;";

    public final static String SQL_START_LOG_MINER_AUTO_ADD_LOG_10 = "" +
            "BEGIN SYS.DBMS_LOGMNR.START_LOGMNR(" +
            "   STARTSCN => ?," +
            "   OPTIONS => SYS.DBMS_LOGMNR.SKIP_CORRUPTION " +
            "       + SYS.DBMS_LOGMNR.NO_SQL_DELIMITER " +
            "       + SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT " +
            "       + SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG " +
            "       + SYS.DBMS_LOGMNR.CONTINUOUS_MINE " +
            "       + SYS.DBMS_LOGMNR.COMMITTED_DATA_ONLY " +
            ");" +
            "END;";


    public final static String SQL_QUERY_LOG_FILE =
            "SELECT\n" +
                    "    MIN(name) as name,\n" +
                    "    MIN(thread#) as thread#,\n" +
                    "    first_change#,\n" +
                    "    MIN(next_change#) as next_change#,\n" +
                    "    MIN(BYTES) as BYTES\n" +
            "FROM\n" +
            "    (\n" +
            "        SELECT\n" +
            "            member AS name,\n" +
            "            thread#,\n" +
            "            first_change#,\n" +
            "            next_change#,\n" +
            "            BYTES\n" +
            "        FROM\n" +
            "            v$log       l\n" +
            "            INNER JOIN v$logfile   f ON l.group# = f.group#\n" +
            "        WHERE l.STATUS = 'CURRENT' OR l.STATUS = 'ACTIVE'\n" +
            "        UNION\n" +
            "        SELECT\n" +
            "            name,\n" +
            "            thread#,\n" +
            "            first_change#,\n" +
            "            next_change#,\n" +
            "            BLOCKS*BLOCK_SIZE as BYTES\n" +
            "        FROM\n" +
            "            v$archived_log\n" +
            "        WHERE\n" +
            "            name IS NOT NULL\n" +
            "            AND STANDBY_DEST='NO' \n" +
            "    )\n" +
            "WHERE\n" +
            "    first_change# >= ?\n" +
            "    OR ? < next_change#\n" +
            "        GROUP BY\n" +
            "            first_change#\n" +
            "ORDER BY\n" +
            "    first_change#";

    public final static String SQL_QUERY_LOG_FILE_10 =
            "SELECT\n" +
                    "    MIN(name) as name,\n" +
                    "    MIN(thread#) as thread#,\n" +
                    "    first_change#,\n" +
                    "    MIN(next_change#) as next_change#,\n" +
                    "    MIN(BYTES) as BYTES\n" +
            "FROM\n" +
            "    (\n" +
            "        SELECT\n" +
            "            member as  name,\n" +
            "            thread#,\n" +
            "            first_change#,\n" +
            "            281474976710655 AS next_change#,\n" +
            "            BYTES\n" +
            "        FROM\n" +
            "            v$log       l\n" +
            "            INNER JOIN v$logfile   f ON l.group# = f.group#\n" +
            "        WHERE l.STATUS = 'CURRENT' OR l.STATUS = 'ACTIVE'\n" +
            "        UNION\n" +
            "        SELECT\n" +
            "            name,\n" +
            "            thread#,\n" +
            "            first_change#,\n" +
            "            next_change#,\n" +
            "            BLOCKS*BLOCK_SIZE as BYTES\n" +
            "        FROM\n" +
            "            v$archived_log\n" +
            "        WHERE\n" +
            "            name IS NOT NULL\n" +
            "            AND STANDBY_DEST='NO' \n" +
            "    )\n" +
            "WHERE\n" +
            "    first_change# >= ?\n" +
            "    OR ? < next_change#\n" +
            "        GROUP BY\n" +
            "            first_change#\n" +
            "ORDER BY\n" +
            "    first_change#";

    public final static String SQL_SELECT_DATA = "" +
            "SELECT\n" +
            "    scn,\n" +
            //oracle 10 没有该字段
//            "    commit_scn,\n" +
            "    timestamp,\n" +
            "    operation,\n" +
            "    operation_code,\n" +
            "    seg_owner,\n" +
            "    table_name,\n" +
            "    sql_redo,\n" +
            "    sql_undo,\n" +
            "    xidsqn,\n" +
            "    row_id,\n" +
            "    rollback,\n" +
            "    csf\n" +
            "FROM\n" +
            "    v$logmnr_contents\n" +
            "WHERE\n" +
            "    scn > ? \n" +
            "    AND scn < ? \n";


    /**
     * 加载包含startSCN和endSCN之间日志的日志文件
     */
    public final static String SQL_START_LOGMINER =  "DECLARE \n" +
            "    st          BOOLEAN := true;\n" +
            "    start_scn   NUMBER := ?;\n" +
            "    endScn   NUMBER := ?;\n" +
            "BEGIN\n" +
            "    FOR l_log_rec IN (\n" +
            "        SELECT\n" +
            "            MIN(name) name,\n" +
            "            first_change#\n" +
            "        FROM\n" +
            "          (\n" +
            "            SELECT \n"+
            "              member AS name, \n"+
            "              first_change# \n"+
            "            FROM \n"+
            "              v$log   l \n"+
            "           INNER JOIN v$logfile   f ON l.group# = f.group# \n"+
            "           WHERE (l.STATUS = 'CURRENT' OR l.STATUS = 'ACTIVE' )\n"+
            "           AND first_change# < endScn \n"+
            "           UNION \n"+
            "           SELECT  \n"+
            "              name, \n"+
            "              first_change# \n"+
            "           FROM \n"+
            "              v$archived_log \n"+
            "           WHERE \n"+
            "              name IS NOT NULL \n"+
            "           AND STANDBY_DEST='NO'\n"+
            "           AND  first_change# < endScn  AND next_change# > start_scn )group by first_change# order by first_change#  )LOOP IF st THEN \n"+
            "  SYS.DBMS_LOGMNR.add_logfile(l_log_rec.name, SYS.DBMS_LOGMNR.new); \n"+
            "      st := false; \n"+
            "  ELSE \n"+
            "  SYS.DBMS_LOGMNR.add_logfile(l_log_rec.name); \n"+
            "  END IF; \n"+
            "  END LOOP;\n"+
            "  SYS.DBMS_LOGMNR.start_logmnr(       options =>          SYS.DBMS_LOGMNR.skip_corruption        + SYS.DBMS_LOGMNR.no_sql_delimiter        + SYS.DBMS_LOGMNR.no_rowid_in_stmt\n"+
            "  + SYS.DBMS_LOGMNR.dict_from_online_catalog    );\n"+
            "   end;";


    /** 查找delete的rollback语句对应的insert语句  存在一个事务里rowid相同的其他语句 所以需要子查询过滤掉scn相同rowid相同的语句(这是一对rollback和DML)*/
    public static String queryDataForRollback =
            "SELECT\n" +
                    "    scn,\n" +
                    //oracle 10 没有该字段
//            "    commit_scn,\n" +
                    "    timestamp,\n" +
                    "    operation,\n" +
                    "    operation_code,\n" +
                    "    seg_owner,\n" +
                    "    table_name,\n" +
                    "    sql_redo,\n" +
                    "    sql_undo,\n" +
                    "    xidsqn,\n" +
                    "    row_id,\n" +
                    "    rollback,\n" +
                    "    csf\n" +
            "FROM\n" +
            "   v$logmnr_contents a \n"+
            "where \n"+
                "scn <=?  and row_id=?  and xidsqn = ? and table_name = ?  and rollback =? and OPERATION_CODE =? \n"+
                "and scn not in (select scn from  v$logmnr_contents where row_id =  ?   and xidsqn = ?  and scn !=?  group by scn HAVING count(scn) >1 and sum(rollback)>0) \n";

    //查找加载到logminer的日志文件
    public final static String SQL_QUERY_ADDED_LOG = "select filename ,thread_id ,low_scn,next_scn,type,filesize,status,type from  V$LOGMNR_LOGS ";

    //移除logminer加载的日志文件
    public final static String SQL_REMOVE_ADDED_LOG = "SYS.DBMS_LOGMNR.add_logfile(LogFileName=>?, Options=>dbms_logmnr.removefile) ";



    public final static String SQL_STOP_LOG_MINER = "BEGIN SYS.DBMS_LOGMNR.END_LOGMNR; end;";

    public final static String SQL_GET_CURRENT_SCN = "select min(CURRENT_SCN) CURRENT_SCN from gv$database";

    public final static String SQL_GET_MAX_SCN_IN_CONTENTS = "select max(scn) as scn  from  v$logmnr_contents";

    public final static String SQL_GET_LOG_FILE_START_POSITION = "select min(FIRST_CHANGE#) FIRST_CHANGE# from (select FIRST_CHANGE# from v$log union select FIRST_CHANGE# from v$archived_log where standby_dest='NO' and name is not null)";

    public final static String SQL_GET_LOG_FILE_START_POSITION_BY_SCN = "select min(FIRST_CHANGE#) FIRST_CHANGE# from (select FIRST_CHANGE# from v$log where ? between FIRST_CHANGE# and NEXT_CHANGE# union select FIRST_CHANGE# from v$archived_log where ? between FIRST_CHANGE# and NEXT_CHANGE# and standby_dest='NO'  and name is not null)";

    public final static String SQL_GET_LOG_FILE_START_POSITION_BY_SCN_10 = "select min(FIRST_CHANGE#) FIRST_CHANGE# from (select FIRST_CHANGE# from v$log where ? > FIRST_CHANGE# union select FIRST_CHANGE# from v$archived_log where ? between FIRST_CHANGE# and NEXT_CHANGE# and standby_dest='NO' and name is not null)";

    public final static String SQL_GET_LOG_FILE_START_POSITION_BY_TIME = "select min(FIRST_CHANGE#) FIRST_CHANGE# from (select FIRST_CHANGE# from v$log where TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS') between FIRST_TIME and NVL(NEXT_TIME, TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')) union select FIRST_CHANGE# from v$archived_log where TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS') between FIRST_TIME and NEXT_TIME and standby_dest='NO' and name is not null)";

    public final static String SQL_GET_LOG_FILE_START_POSITION_BY_TIME_10 = "select min(FIRST_CHANGE#) FIRST_CHANGE# from (select FIRST_CHANGE# from v$log where TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS') > FIRST_TIME union select FIRST_CHANGE# from v$archived_log where TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS') between FIRST_TIME and NEXT_TIME and standby_dest='NO' and name is not null)";

    //查询会话级别 NLS_DATE_FORMAT 参数值
    public final static String SQL_QUERY_NLS_DATE_FORMAT="select VALUE from nls_session_parameters where parameter = 'NLS_DATE_FORMAT'";

    //修改当前会话的date日期格式
    public final static String SQL_ALTER_DATE_FORMAT ="ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'";

    //修改当前会话的timestamp日期格式
    public final static String NLS_TIMESTAMP_FORMAT ="ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6'";

    public final static String SQL_QUERY_ROLES = "SELECT * FROM USER_ROLE_PRIVS";

    public final static String SQL_QUERY_PRIVILEGES = "SELECT * FROM SESSION_PRIVS";

    public final static String SQL_QUERY_ENCODING = "SELECT USERENV('LANGUAGE') FROM DUAL";

    public final static String SQL_QUERY_LOG_MODE = "SELECT LOG_MODE FROM V$DATABASE";

    public final static String SQL_QUERY_SUPPLEMENTAL_LOG_DATA_ALL = "SELECT SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE";

    private final static List<String> SUPPORTED_OPERATIONS = Arrays.asList("UPDATE", "INSERT", "DELETE");

    public static List<String> EXCLUDE_SCHEMAS = Collections.singletonList("SYS");

    public static final List<String> PRIVILEGES_NEEDED = Arrays.asList("CREATE SESSION", "LOGMINING", "SELECT ANY TRANSACTION", "SELECT ANY DICTIONARY");

    public static final List<String> ORACLE_11_PRIVILEGES_NEEDED = Arrays.asList("CREATE SESSION", "SELECT ANY TRANSACTION", "SELECT ANY DICTIONARY");

    /**
     * 构建查询v$logmnr_contents视图SQL
     * @param listenerOptions   需要采集操作类型字符串 delete,insert,update
     * @param listenerTables    需要采集的schema+表名 SCHEMA1.TABLE1,SCHEMA2.TABLE2
     * @return
     */
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

    /**
     * 构建需要采集操作类型字符串的过滤条件
     * @param listenerOptions 需要采集操作类型字符串 delete,insert,update
     * @return
     */
    private static String buildOperationFilter(String listenerOptions){
        List<String> standardOperations = new ArrayList<>();

        String[] operations = listenerOptions.split(ConstantValue.COMMA_SYMBOL);
        for (String operation : operations) {

            int operationCode;
            switch (operation.toUpperCase()) {
                case "INSERT":
                    operationCode = 1;
                    break;
                case "DELETE":
                    operationCode = 2;
                    break;
                case "UPDATE":
                    operationCode = 3;
                    break;
                default:
                    throw new RuntimeException("Unsupported operation type:" + operation);
            }

            standardOperations.add(String.format("'%s'", operationCode));
        }

        return String.format("OPERATION_CODE in (%s) ", StringUtils.join(standardOperations, ConstantValue.COMMA_SYMBOL));
    }

    /**
     * 过滤系统表
     * @return
     */
    private static String buildExcludeSchemaFilter(){
        List<String> filters = new ArrayList<>();
        for (String excludeSchema : EXCLUDE_SCHEMAS) {
            filters.add(String.format("SEG_OWNER != '%s'", excludeSchema));
        }

        return String.format("(%s)", StringUtils.join(filters, " and "));
    }

    /**
     * 构建需要采集的schema+表名的过滤条件
     * @param listenerTables    需要采集的schema+表名 SCHEMA1.TABLE1,SCHEMA2.TABLE2
     * @return
     */
    private static String buildSchemaTableFilter(String listenerTables){
        List<String> filters = new ArrayList<>();

        String[] tableWithSchemas = listenerTables.split(ConstantValue.COMMA_SYMBOL);
        for (String tableWithSchema : tableWithSchemas){
            List<String> tables = Arrays.asList(tableWithSchema.split("\\."));
            if (ConstantValue.STAR_SYMBOL.equals(tables.get(0))) {
                throw new IllegalArgumentException("Must specify the schema to be collected:" + tableWithSchema);
            }

            StringBuilder tableFilterBuilder = new StringBuilder(256);
            tableFilterBuilder.append(String.format("SEG_OWNER='%s'", tables.get(0)));

            if(!ConstantValue.STAR_SYMBOL.equals(tables.get(1))){
                tableFilterBuilder.append(" and ").append(String.format("TABLE_NAME='%s'", tables.get(1)));
            }

            filters.add(String.format("(%s)", tableFilterBuilder.toString()));
        }

        return String.format("(%s)", StringUtils.join(filters, " or "));
    }

    /**
     * 是否为临时表，临时表没有redo sql，sql_redo内容为No SQL_UNDO for temporary tables
     * @param sql redo sql
     * @return
     */
    public static boolean isCreateTemporaryTableSql(String sql) {
        return sql.contains("temporary tables");
    }
}
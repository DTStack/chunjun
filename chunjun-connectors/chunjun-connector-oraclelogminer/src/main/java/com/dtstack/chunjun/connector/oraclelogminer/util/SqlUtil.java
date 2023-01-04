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
package com.dtstack.chunjun.connector.oraclelogminer.util;

import com.dtstack.chunjun.connector.oraclelogminer.entity.ColumnInfo;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.DateUtil;

import org.apache.flink.types.RowKind;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

@Slf4j
public class SqlUtil {

    /**
     * OPTIONS参数说明: DBMS_LOGMNR.SKIP_CORRUPTION - 跳过出错的redlog DBMS_LOGMNR.NO_SQL_DELIMITER - 不使用
     * ';'分割redo sql DBMS_LOGMNR.NO_ROWID_IN_STMT -
     * 默认情况下，用于UPDATE和DELETE操作的SQL_REDO和SQL_UNDO语句在where子句中包含“ ROWID =”。
     * 但是，这对于想要重新执行SQL语句的应用程序是不方便的。设置此选项后，“ ROWID”不会放置在重构语句的末尾 DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
     * - 使用在线字典 DBMS_LOGMNR.CONTINUOUS_MINE - 需要在生成重做日志的同一实例中使用日志 DBMS_LOGMNR.COMMITTED_DATA_ONLY -
     * 指定此选项时，LogMiner将属于同一事务的所有DML操作分组在一起。事务按提交顺序返回。 DBMS_LOGMNR.STRING_LITERALS_IN_STMT -
     * 默认情况下，格式化格式化的SQL语句时，SQL_REDO和SQL_UNDO语句会使用数据库会话的NLS设置
     * 例如NLS_DATE_FORMAT，NLS_NUMERIC_CHARACTERS等）。使用此选项，将使用ANSI / ISO字符串文字格式对重构的SQL语句进行格式化。
     */
    public static final String SQL_START_LOG_MINER_AUTO_ADD_LOG =
            ""
                    + "BEGIN SYS.DBMS_LOGMNR.START_LOGMNR("
                    + "   STARTSCN => ?,"
                    + "   OPTIONS => SYS.DBMS_LOGMNR.SKIP_CORRUPTION "
                    + "       + SYS.DBMS_LOGMNR.NO_SQL_DELIMITER "
                    + "       + SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT "
                    + "       + SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG "
                    + "       + SYS.DBMS_LOGMNR.CONTINUOUS_MINE "
                    + "       + SYS.DBMS_LOGMNR.COMMITTED_DATA_ONLY "
                    + "       + SYS.DBMS_LOGMNR.STRING_LITERALS_IN_STMT"
                    + ");"
                    + "END;";

    public static final String SQL_START_LOG_MINER_AUTO_ADD_LOG_10 =
            ""
                    + "BEGIN SYS.DBMS_LOGMNR.START_LOGMNR("
                    + "   STARTSCN => ?,"
                    + "   OPTIONS => SYS.DBMS_LOGMNR.SKIP_CORRUPTION "
                    + "       + SYS.DBMS_LOGMNR.NO_SQL_DELIMITER "
                    + "       + SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT "
                    + "       + SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG "
                    + "       + SYS.DBMS_LOGMNR.CONTINUOUS_MINE "
                    + "       + SYS.DBMS_LOGMNR.COMMITTED_DATA_ONLY "
                    + ");"
                    + "END;";
    public static final String SQL_QUERY_LOG_FILE =
            "SELECT\n"
                    + "    MIN(name) as name,\n"
                    + "    MIN(thread#) as thread#,\n"
                    + "    first_change#,\n"
                    + "    MIN(next_change#) as next_change#,\n"
                    + "    MIN(BYTES) as BYTES,\n"
                    + "    MIN(TYPE) as TYPE\n"
                    + "FROM\n"
                    + "    (\n"
                    + "        SELECT\n"
                    + "            member AS name,\n"
                    + "            thread#,\n"
                    + "            first_change#,\n"
                    + "            next_change#,\n"
                    + "            BYTES,\n"
                    + "            'ONLINE' as TYPE\n"
                    + "        FROM\n"
                    + "            v$log       l\n"
                    + "            INNER JOIN v$logfile   f ON l.group# = f.group#\n"
                    + "        WHERE l.STATUS = 'CURRENT' OR l.STATUS = 'ACTIVE'\n"
                    + "        UNION\n"
                    + "        SELECT\n"
                    + "            name,\n"
                    + "            thread#,\n"
                    + "            first_change#,\n"
                    + "            next_change#,\n"
                    + "            BLOCKS*BLOCK_SIZE as BYTES,\n"
                    + "            'ARCHIVE' as TYPE\n"
                    + "        FROM\n"
                    + "            v$archived_log\n"
                    + "        WHERE\n"
                    + "            name IS NOT NULL\n"
                    + "            AND STANDBY_DEST='NO' \n"
                    + "    )\n"
                    + "WHERE\n"
                    + "    first_change# >= ?\n"
                    + "    OR ? < next_change#\n"
                    + "        GROUP BY\n"
                    + "            first_change#\n"
                    + "ORDER BY\n"
                    + "    first_change#";

    public static final String SQL_QUERY_ARCHIVE_LOG_FILE =
            "SELECT\n"
                    + "    MIN(name) as name,\n"
                    + "    MIN(thread#) as thread#,\n"
                    + "    first_change#,\n"
                    + "    MIN(next_change#) as next_change#,\n"
                    + "    MIN(BYTES) as BYTES,\n"
                    + "    'ARCHIVE' as TYPE\n"
                    + "FROM\n"
                    + "    (\n"
                    + "        SELECT\n"
                    + "            name,\n"
                    + "            thread#,\n"
                    + "            first_change#,\n"
                    + "            next_change#,\n"
                    + "            BLOCKS*BLOCK_SIZE as BYTES\n"
                    + "        FROM\n"
                    + "            v$archived_log\n"
                    + "        WHERE\n"
                    + "            name IS NOT NULL\n"
                    + "            AND STANDBY_DEST='NO' \n"
                    + "    )\n"
                    + "WHERE\n"
                    + "    first_change# >= ?\n"
                    + "    OR ? < next_change#\n"
                    + "        GROUP BY\n"
                    + "            first_change#\n"
                    + "ORDER BY\n"
                    + "    first_change#";

    public static final String SQL_QUERY_LOG_FILE_10 =
            "SELECT\n"
                    + "    MIN(name) as name,\n"
                    + "    MIN(thread#) as thread#,\n"
                    + "    first_change#,\n"
                    + "    MIN(next_change#) as next_change#,\n"
                    + "    MIN(BYTES) as BYTES,\n"
                    + "    MIN(TYPE) as TYPE\n"
                    + "FROM\n"
                    + "    (\n"
                    + "        SELECT\n"
                    + "            member as  name,\n"
                    + "            thread#,\n"
                    + "            first_change#,\n"
                    + "            281474976710655 AS next_change#,\n"
                    + "            BYTES,\n"
                    + "            'ONLINE' as TYPE\n"
                    + "        FROM\n"
                    + "            v$log       l\n"
                    + "            INNER JOIN v$logfile   f ON l.group# = f.group#\n"
                    + "        WHERE l.STATUS = 'CURRENT' OR l.STATUS = 'ACTIVE'\n"
                    + "        UNION\n"
                    + "        SELECT\n"
                    + "            name,\n"
                    + "            thread#,\n"
                    + "            first_change#,\n"
                    + "            next_change#,\n"
                    + "            BLOCKS*BLOCK_SIZE as BYTES,\n"
                    + "            'ARCHIVE' as TYPE\n"
                    + "        FROM\n"
                    + "            v$archived_log\n"
                    + "        WHERE\n"
                    + "            name IS NOT NULL\n"
                    + "            AND STANDBY_DEST='NO' \n"
                    + "    )\n"
                    + "WHERE\n"
                    + "    first_change# >= ?\n"
                    + "    OR ? < next_change#\n"
                    + "        GROUP BY\n"
                    + "            first_change#\n"
                    + "ORDER BY\n"
                    + "    first_change#";

    public static final String SQL_QUERY_ARCHIVE_LOG_FILE_10 =
            "SELECT\n"
                    + "    MIN(name) as name,\n"
                    + "    MIN(thread#) as thread#,\n"
                    + "    first_change#,\n"
                    + "    MIN(next_change#) as next_change#,\n"
                    + "    MIN(BYTES) as BYTES,\n"
                    + "    'ARCHIVE' as TYPE\n"
                    + "FROM\n"
                    + " ("
                    + "        SELECT\n"
                    + "            name,\n"
                    + "            thread#,\n"
                    + "            first_change#,\n"
                    + "            next_change#,\n"
                    + "            BLOCKS*BLOCK_SIZE as BYTES\n"
                    + "        FROM\n"
                    + "            v$archived_log\n"
                    + "        WHERE\n"
                    + "            name IS NOT NULL\n"
                    + "            AND STANDBY_DEST='NO' \n"
                    + "    )\n"
                    + "WHERE\n"
                    + "    first_change# >= ?\n"
                    + "    OR ? < next_change#\n"
                    + "        GROUP BY\n"
                    + "            first_change#\n"
                    + "ORDER BY\n"
                    + "    first_change#";
    public static final String SQL_SELECT_DATA =
            ""
                    + "SELECT\n"
                    + "    scn,\n"
                    +
                    // oracle 10 没有该字段
                    //            "    commit_scn,\n" +
                    "    timestamp,\n"
                    + "    operation,\n"
                    + "    operation_code,\n"
                    + "    seg_owner,\n"
                    + "    table_name,\n"
                    + "    sql_redo,\n"
                    + "    sql_undo,\n"
                    + "    xidusn,\n"
                    + "    xidslt,\n"
                    + "    xidsqn,\n"
                    + "    row_id,\n"
                    + "    rollback,\n"
                    + "    csf\n"
                    + "FROM\n"
                    + "    v$logmnr_contents\n"
                    + "WHERE\n"
                    + "    scn >= ? \n"
                    + "    AND scn < ? \n";
    /** 加载包含startSCN和endSCN之间日志的日志文件 */
    public static final String SQL_START_LOGMINER =
            "DECLARE \n"
                    + "    st          BOOLEAN := true;\n"
                    + "    start_scn   NUMBER := ?;\n"
                    + "    endScn   NUMBER := ?;\n"
                    + "BEGIN\n"
                    + "    FOR l_log_rec IN (\n"
                    + "        SELECT\n"
                    + "            MIN(name) name,\n"
                    + "            first_change#\n"
                    + "        FROM\n"
                    + "          (\n"
                    + "            SELECT \n"
                    + "              member AS name, \n"
                    + "              first_change# \n"
                    + "            FROM \n"
                    + "              v$log   l \n"
                    + "           INNER JOIN v$logfile   f ON l.group# = f.group# \n"
                    + "           WHERE (l.STATUS = 'CURRENT' OR l.STATUS = 'ACTIVE' )\n"
                    + "           AND first_change# < endScn \n"
                    + "           UNION \n"
                    + "           SELECT  \n"
                    + "              name, \n"
                    + "              first_change# \n"
                    + "           FROM \n"
                    + "              v$archived_log \n"
                    + "           WHERE \n"
                    + "              name IS NOT NULL \n"
                    + "           AND STANDBY_DEST='NO'\n"
                    + "           AND  first_change# < endScn  AND next_change# > start_scn )group by first_change# order by first_change#  )LOOP IF st THEN \n"
                    + "  SYS.DBMS_LOGMNR.add_logfile(l_log_rec.name, SYS.DBMS_LOGMNR.new); \n"
                    + "      st := false; \n"
                    + "  ELSE \n"
                    + "  SYS.DBMS_LOGMNR.add_logfile(l_log_rec.name); \n"
                    + "  END IF; \n"
                    + "  END LOOP;\n"
                    + "  SYS.DBMS_LOGMNR.start_logmnr(       options =>          SYS.DBMS_LOGMNR.skip_corruption        + SYS.DBMS_LOGMNR.no_sql_delimiter        + SYS.DBMS_LOGMNR.no_rowid_in_stmt\n"
                    + "  + SYS.DBMS_LOGMNR.dict_from_online_catalog    );\n"
                    + "   end;";

    public static final String SQL_QUERY_TABLE_COLUMN_INFO_TEMPLATE =
            ""
                    + "SELECT a.OWNER          schema        --schema\n"
                    + "     , a.TABLE_NAME     tableName     --table\n"
                    + "     , b.column_name    columnName    --字段名\n"
                    + "     , b.data_type      dataType      --字段类型\n"
                    + "     , b.data_precision dataPrecision --字段长度\n"
                    + "     , b.CHAR_LENGTH    charLength    --字段长度\n"
                    + "     , b.DATA_LENGTH     dataLength    --字段长度\n"
                    + "     , b.DATA_SCALE     dataScale     --字段精度\n"
                    + "     , b.DATA_DEFAULT   defaultValue  --默认值\n"
                    + "     , b.NULLABLE       nullable      --是否可为空\n"
                    + "     , a.comments       columnComment --字段注释\n"
                    + "     , c.comments       tableComment --表注释\n"
                    + "FROM all_col_comments a --字段注释表\n"
                    + "   , all_tab_columns b  --字段列表\n"
                    + "   , all_tab_comments c  --表注释表\n"
                    + "WHERE a.TABLE_NAME = b.TABLE_NAME\n"
                    + "  and a.OWNER = b.OWNER\n"
                    + "  and a.OWNER = c.OWNER\n"
                    + "  and a.TABLE_NAME = c.TABLE_NAME\n"
                    + "  and a.COLUMN_NAME = b.COLUMN_NAME\n"
                    + "%s";

    // 查找加载到logminer的日志文件
    public static final String SQL_QUERY_ADDED_LOG =
            "select filename ,thread_id ,low_scn,next_scn,type,filesize,status,type from  V$LOGMNR_LOGS ";
    // 移除logminer加载的日志文件
    public static final String SQL_REMOVE_ADDED_LOG =
            "SYS.DBMS_LOGMNR.add_logfile(LogFileName=>?, Options=>dbms_logmnr.removefile) ";
    public static final String SQL_STOP_LOG_MINER = "BEGIN SYS.DBMS_LOGMNR.END_LOGMNR; end;";
    public static final String SQL_GET_CURRENT_SCN =
            "select min(CURRENT_SCN) CURRENT_SCN from gv$database";
    public static final String SQL_GET_MAX_SCN_IN_CONTENTS =
            "select max(scn) as scn  from  v$logmnr_contents";
    public static final String SQL_GET_LOG_FILE_START_POSITION =
            "select min(FIRST_CHANGE#) FIRST_CHANGE# from (select FIRST_CHANGE# from v$log l where l.STATUS = 'CURRENT' OR l.STATUS = 'ACTIVE' union select FIRST_CHANGE# from v$archived_log where standby_dest='NO' and name is not null)";
    public static final String SQL_GET_LOG_FILE_START_POSITION_BY_TIME =
            "select min(FIRST_CHANGE#) FIRST_CHANGE# from (select FIRST_CHANGE# from v$log where TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS') between FIRST_TIME and NVL(NEXT_TIME, TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')) union select FIRST_CHANGE# from v$archived_log where TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS') between FIRST_TIME and NEXT_TIME and standby_dest='NO' and name is not null)";
    public static final String SQL_GET_LOG_FILE_START_POSITION_BY_TIME_10 =
            "select min(FIRST_CHANGE#) FIRST_CHANGE# from (select FIRST_CHANGE# from v$log where TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS') > FIRST_TIME union select FIRST_CHANGE# from v$archived_log where TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS') between FIRST_TIME and NEXT_TIME and standby_dest='NO' and name is not null)";
    // 修改当前会话的date日期格式
    public static final String SQL_ALTER_NLS_SESSION_PARAMETERS =
            "ALTER SESSION SET "
                    + "  NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
                    + "  NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6'"
                    + "  NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6'";
    public static final String SQL_QUERY_ROLES = "SELECT * FROM USER_ROLE_PRIVS";
    public static final String SQL_QUERY_PRIVILEGES = "SELECT * FROM SESSION_PRIVS";
    public static final String SQL_QUERY_ENCODING = "SELECT USERENV('LANGUAGE') FROM DUAL";
    public static final String SQL_QUERY_LOG_MODE = "SELECT LOG_MODE FROM V$DATABASE";
    public static final String SQL_QUERY_SUPPLEMENTAL_LOG_DATA_ALL =
            "SELECT SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE";
    /** cdb环境切换容器 * */
    public static final String SQL_ALTER_SESSION_CONTAINER = "alter session set container=%s";

    public static final String SQL_IS_CDB = "select cdb from v$database";
    public static final String SQL_IS_RAC =
            " select VALUE from v$option a where a.PARAMETER='Real Application Clusters'";
    public static final List<String> PRIVILEGES_NEEDED =
            Arrays.asList(
                    "CREATE SESSION",
                    "LOGMINING",
                    "SELECT ANY TRANSACTION",
                    "SELECT ANY DICTIONARY");
    public static final List<String> ORACLE_11_PRIVILEGES_NEEDED =
            Arrays.asList("CREATE SESSION", "SELECT ANY TRANSACTION", "SELECT ANY DICTIONARY");
    private static final List<String> SUPPORTED_OPERATIONS =
            Arrays.asList("UPDATE", "INSERT", "DELETE");

    /** 查找事务里 回滚数据（delete 和 update）对应的操作数据 */
    public static String queryDataForRollback =
            "SELECT\n"
                    + "    scn,"
                    +
                    // oracle 10 没有该字段
                    //            "    commit_scn,\n" +
                    "    timestamp,"
                    + "    operation,"
                    + "    operation_code,"
                    + "    seg_owner,"
                    + "    table_name,"
                    + "    sql_redo,"
                    + "    sql_undo,"
                    + "    xidusn,"
                    + "    xidslt,"
                    + "    xidsqn,"
                    + "    row_id,"
                    + "    rollback,"
                    + "    csf\n"
                    + "FROM\n"
                    + "   v$logmnr_contents a \n"
                    + "where \n"
                    + " (  xidusn = ?  and xidslt = ? and xidsqn = ? and table_name = ?  and rollback =? and OPERATION_CODE in (?,?)) "
                    + "AND \n"
                    + "    (scn >= ? "
                    + "    AND scn < ?) \n"
                    + "    or (scn = ?) \n";

    public static List<String> EXCLUDE_SCHEMAS = Collections.singletonList("SYS");

    /**
     * 构建查询v$logmnr_contents视图SQL
     *
     * @param listenerOptions 需要采集操作类型字符串 delete,insert,update
     * @param listenerTables 需要采集的schema+表名 SCHEMA1.TABLE1,SCHEMA2.TABLE2
     * @return
     */
    public static String buildSelectSql(
            String listenerOptions, boolean ddlSkip, String listenerTables, boolean isCdb) {
        StringBuilder sqlBuilder = new StringBuilder(SQL_SELECT_DATA);
        sqlBuilder.append(" and ( ");
        if (StringUtils.isNotEmpty(listenerTables)) {
            sqlBuilder.append(buildSchemaTableFilter(listenerTables, isCdb));
        } else {
            sqlBuilder.append(buildExcludeSchemaFilter());
        }

        sqlBuilder.append(" and ").append(buildOperationFilter(listenerOptions, ddlSkip));

        // 包含commit 以及 rollback
        sqlBuilder.append(" or  OPERATION_CODE in (7, 36)");

        sqlBuilder.append(")");

        String sql = sqlBuilder.toString();
        log.info("SelectSql = {}", sql);
        return sql;
    }

    /**
     * 构建需要采集操作类型字符串的过滤条件
     *
     * @param listenerOptions 需要采集操作类型字符串 delete,insert,update
     * @param ddlSkip 需要采集ddl数据
     * @return
     */
    private static String buildOperationFilter(String listenerOptions, boolean ddlSkip) {
        List<Integer> standardOperations = new ArrayList<>();

        if (!ddlSkip) {
            standardOperations.add(5);
        }
        if (StringUtils.isNotEmpty(listenerOptions)) {
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

                standardOperations.add(operationCode);
            }
        } else {
            standardOperations.add(1);
            standardOperations.add(2);
            standardOperations.add(3);
        }
        return String.format(
                "OPERATION_CODE in (%s) ",
                StringUtils.join(standardOperations, ConstantValue.COMMA_SYMBOL));
    }

    /**
     * 过滤系统表
     *
     * @return
     */
    private static String buildExcludeSchemaFilter() {
        List<String> filters = new ArrayList<>();
        for (String excludeSchema : EXCLUDE_SCHEMAS) {
            filters.add(String.format("SEG_OWNER != '%s'", excludeSchema));
        }

        return String.format("(%s)", StringUtils.join(filters, " and "));
    }

    /**
     * 构建需要采集的schema+表名的过滤条件
     *
     * @param listenerTables 需要采集的schema+表名 SCHEMA1.TABLE1,SCHEMA2.TABLE2
     * @return
     */
    private static String buildSchemaTableFilter(String listenerTables, boolean isCdb) {
        List<String> filters = new ArrayList<>();

        String[] tableWithSchemas = listenerTables.split(ConstantValue.COMMA_SYMBOL);
        for (String tableWithSchema : tableWithSchemas) {
            List<String> tables = Arrays.asList(tableWithSchema.split("\\."));
            if (ConstantValue.STAR_SYMBOL.equals(tables.get(0))) {
                throw new IllegalArgumentException(
                        "Must specify the schema to be collected:" + tableWithSchema);
            }

            StringBuilder tableFilterBuilder = new StringBuilder(256);
            if (isCdb && tables.size() == 3) {
                tableFilterBuilder.append(String.format("SRC_CON_NAME='%s' and ", tables.get(0)));
            }

            tableFilterBuilder.append(
                    String.format(
                            "SEG_OWNER='%s'",
                            isCdb && tables.size() == 3 ? tables.get(1) : tables.get(0)));

            if (!ConstantValue.STAR_SYMBOL.equals(
                    isCdb && tables.size() == 3 ? tables.get(2) : tables.get(1))) {
                tableFilterBuilder
                        .append(" and ")
                        .append(
                                String.format(
                                        "TABLE_NAME='%s'",
                                        isCdb && tables.size() == 3
                                                ? tables.get(2)
                                                : tables.get(1)));
            }

            filters.add(String.format("(%s)", tableFilterBuilder));
        }

        return String.format("(%s)", StringUtils.join(filters, " or "));
    }

    /**
     * 是否为临时表，临时表没有redo sql，sql_redo内容为No SQL_UNDO for temporary tables
     *
     * @param sql redo sql
     * @return
     */
    public static boolean isCreateTemporaryTableSql(String sql) {
        return sql.contains("temporary tables");
    }

    public static String quote(String data, String quote) {
        return quote + data + quote;
    }

    public static String getSql(String schema, String table, List<ColumnInfo> columnInfos) {
        String collect =
                columnInfos.stream().map(ColumnInfo::conventToSql).collect(Collectors.joining(","));
        return "CREATE TABLE "
                + quote(schema, "\"")
                + "."
                + quote(table, "\"")
                + " ("
                + collect
                + " )";
    }

    public static String getTableCommentSql(String schema, String table, String comment) {
        return "comment on table "
                + quote(schema, "\"")
                + "."
                + quote(table, "\"")
                + " IS "
                + quote(comment, "'");
    }

    public static String formatGetTableInfoSql(List<Pair<String, String>> tables) {
        String where = " and ";
        if (tables.size() == 1) {
            where += " a.OWNER = ?\n" + " and a.TABLE_NAME = ?";
        } else {
            where =
                    where
                            + "("
                            + tables.stream()
                                    .map(
                                            i ->
                                                    String.format(
                                                            "(a.OWNER = '%s' and a.TABLE_NAME = '%s')",
                                                            i.getLeft(), i.getRight()))
                                    .collect(Collectors.joining("OR"))
                            + ")";
        }
        return String.format(SQL_QUERY_TABLE_COLUMN_INFO_TEMPLATE, where);
    }

    public static String formatGetTableInfoSql(String schema, String table) {
        String where =
                " and "
                        + " a.OWNER = "
                        + "'"
                        + schema
                        + "'"
                        + " and a.TABLE_NAME = "
                        + "'"
                        + table
                        + "'";
        return String.format(SQL_QUERY_TABLE_COLUMN_INFO_TEMPLATE, where);
    }

    public static String formatLockTableWithRowShare(String table) {
        return "lock table " + table + " IN ROW SHARE MODE";
    }

    public static String queryDataByScn(String tableWithSchema, BigInteger scn) {
        String sql = "select * from %s as of scn %d";
        return String.format(sql, tableWithSchema, scn);
    }

    public static String releaseTableLock() {
        return "rollback";
    }

    /* convert resultSet to columnRowData */
    public static List<ColumnRowData> jdbcColumnRowColumnConvert(
            List<ColumnInfo> columnInfos, ResultSet rs) {
        List<ColumnRowData> columnRowDatas = new ArrayList<>();
        try {
            while (rs.next()) {
                ColumnRowData columnRowData = new ColumnRowData(RowKind.INSERT, columnInfos.size());
                for (ColumnInfo columnInfo : columnInfos) {
                    Object value = rs.getObject(columnInfo.getName());
                    columnRowData.addHeader(columnInfo.getName());
                    columnRowData.addField(convertJdbcType(columnInfo.getType(), value));
                }
                columnRowDatas.add(columnRowData);
            }
            return columnRowDatas;
        } catch (Exception e) {
            throw new ChunJunRuntimeException(e);
        }
    }

    private static AbstractBaseColumn convertJdbcType(String tpe, Object val) {
        String substring = tpe;
        int index = tpe.indexOf(ConstantValue.LEFT_PARENTHESIS_SYMBOL);
        if (index > 0) {
            substring = tpe.substring(0, index);
        }

        switch (substring.toUpperCase(Locale.ENGLISH)) {
            case "NUMBER":
            case "SMALLINT":
            case "INT":
            case "INTEGER":
            case "FLOAT":
            case "DECIMAL":
            case "NUMERIC":
            case "BINARY_FLOAT":
            case "BINARY_DOUBLE":
                return new BigDecimalColumn((BigDecimal) val);

            case "CHAR":
            case "NCHAR":
            case "NVARCHAR2":
            case "ROWID":
            case "VARCHAR2":
            case "VARCHAR":
            case "LONG":
            case "RAW":
            case "LONG RAW":
            case "INTERVAL YEAR":
            case "INTERVAL DAY":
            case "BLOB":
            case "CLOB":
            case "NCLOB":
                return new StringColumn((String) val);

            case "DATE":
                if (val instanceof Timestamp) {
                    String formatTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(val);
                    return new TimestampColumn(DateUtil.getTimestampFromStr(formatTime), 0);
                }

                return new TimestampColumn(DateUtil.getTimestampFromStr((String) val), 0);

            case "TIMESTAMP":
                return new TimestampColumn(
                        DateUtil.getTimestampFromStr(((oracle.sql.TIMESTAMP) val).stringValue()),
                        0);

            case "BFILE":
            case "XMLTYPE":
            default:
                throw new UnsupportedOperationException("Unsupported type:" + tpe);
        }
    }
}

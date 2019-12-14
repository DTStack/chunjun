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

/**
 * @author jiangbo
 * @date 2019/12/14
 */
public class LogminerUtil {

    public final static String SQL_START_LOGMINER = "begin \n" +
            "DBMS_LOGMNR.START_LOGMNR(STARTSCN => ?,OPTIONS =>  DBMS_LOGMNR.SKIP_CORRUPTION+DBMS_LOGMNR.NO_SQL_DELIMITER+DBMS_LOGMNR.NO_ROWID_IN_STMT+DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.CONTINUOUS_MINE+DBMS_LOGMNR.COMMITTED_DATA_ONLY+dbms_logmnr.STRING_LITERALS_IN_STMT) \n" +
            "; end;";

    public final static String SQL_SELECT_DATA = "SELECT thread#, scn, start_scn, commit_scn,timestamp, operation_code, operation,status, " +
            "SEG_TYPE_NAME ,info,seg_owner, table_name, username, sql_redo ,row_id, csf, TABLE_SPACE, SESSION_INFO, " +
            "RS_ID, RBASQN, RBABLK, SEQUENCE#, TX_NAME, SEG_NAME, SEG_TYPE_NAME " +
            "FROM  v$logmnr_contents WHERE commit_scn>=? and OPERATION_CODE in (1,2,3) and ((SEG_OWNER='JIANGBO'))";

    public static String buildSelectSql(String listenerOptions, String listenerTables){
        StringBuilder sqlBuilder = new StringBuilder(SQL_SELECT_DATA);

        if (StringUtils.isNotEmpty(listenerOptions)) {

        }

        if (StringUtils.isNotEmpty(listenerTables)) {
            sqlBuilder.append("");
        }

        return sqlBuilder.toString();
    }
}

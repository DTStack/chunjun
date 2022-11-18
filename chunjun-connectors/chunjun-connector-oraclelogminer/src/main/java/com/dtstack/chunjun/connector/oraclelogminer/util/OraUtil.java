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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

public class OraUtil {

    /**
     * 解析LogMiner异常信息，尝试分析原因及给出解决方法
     *
     * @param errorMsg
     * @return Pair L: Cause, R: Solution
     */
    public static Pair<String, String> parseErrorMsg(String errorMsg) {
        Pair<String, String> pair = null;
        if (StringUtils.isNotBlank(errorMsg)) {
            if (errorMsg.contains("ORA-01284")) {
                pair =
                        Pair.of(
                                "The file or directory may not exist or may be inaccessible or pathname exceeds 256 characters",
                                "Please ensure that the file and the directory exist and are accessible");
            } else if (errorMsg.contains("ORA-01013")) {
                pair =
                        Pair.of(
                                "sql is timeout when query data from logMiner",
                                "the default value of logMiner parameter: [queryTimeout] is 300S, it may be useful to adjust [queryTimeout], for example: [\"queryTimeout\": 360]");
            } else if (errorMsg.contains("ORA-00604")) {
                pair =
                        Pair.of(
                                "too many Oracle archive logs queried",
                                "increase the scn number of the query to reduce the number of archive logs queried OR set [readPosition] to [current]");
            } else if (errorMsg.contains("Connection reset by peer")) {
                pair =
                        Pair.of(
                                "This simply means that something in the backend ( DBMS ) decided to stop working due to unavailability of resources etc",
                                "This is not a problem with the ChunJun program, so there is nothing to do but restarting the logMiner task. For more detail, look at this: https://stackoverflow.com/questions/6110395/sqlrecoverableexception-i-o-exception-connection-reset");
            } else if (errorMsg.contains("ORA-00310")) {
                pair =
                        Pair.of(
                                "The archived log was out of sequence, probably because it was corrupt or the wrong redo log file name was specified during recovery",
                                "it may be useful to increase the number and size of redo log groups, and the restart the logMiner task");
            } else if (errorMsg.contains("ORA-01289")) {
                pair =
                        Pair.of(
                                "This simply means that the logfile specified has already been added to the list of logfiles and it may be we find same logfile ",
                                " you can restart the logMiner task");
            } else if (errorMsg.contains("ORA-00308") || errorMsg.contains("ORA-27037")) {
                // todo https://blog.csdn.net/czmmiao/article/details/84173603
                pair =
                        Pair.of(
                                "This simply means that if your Oracle version is 11.1, Oracle will scan or dump the redo records within 12 hours. If it is found that the required redo records have been deleted in the scan or dump, ora-00308 and ora-27037 errors will be reported.  ",
                                "you can restart the logMiner task or The recommended solution for Oracle is to patch 8825048 and upgrade to 11.1.0.7.3 (patch set update) or 11.2.0.1 (base release)");
            }
        }
        return pair;
    }
}

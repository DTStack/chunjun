/**
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

package com.dtstack.flinkx.hdfs.writer;

import com.dtstack.flinkx.rdb.util.DBUtil;
import com.dtstack.flinkx.util.ClassUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Sets;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Hive Utilities
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class HiveUtil {
    private static String QUOTE = "`";
    private static String SLASH = "/";
    private static String COMMA = ",";
    private static String ASSIGN = "=";
    private static String SINGLE_QUOTE = "'";

    private static String quotedString(String str) {
        return QUOTE + str + QUOTE;
    }

    public static Connection getConnection(String url, String username, String password) throws SQLException {
        Connection dbConn = null;
        DriverManager.setLoginTimeout(10);
        ClassUtil.forName("org.apache.hive.jdbc.HiveDriver", HiveUtil.class.getClassLoader());
        if (username == null) {
            dbConn = DriverManager.getConnection(url);
        } else {
            dbConn = DriverManager.getConnection(url, username, password);
        }
        return dbConn;
    }


    public static List<String> getPartitionCols(Connection dbConn, String table) throws SQLException {
        List<String> partitionCols = new ArrayList<>();
        List<Map<String, Object>> result = DBUtil.executeQuery(dbConn, "desc extended " + quotedString(table));
        boolean partitioned = false;
        Iterator<Map<String, Object>> iter = result.iterator();

        while (iter.hasNext()) {
            Map<String, Object> row = iter.next();
            String colName = (String) row.get("col_name");
            if (!partitioned && StringUtils.isNotEmpty(colName) && colName.startsWith("# Partition Information")) {
                partitioned = true;
                continue;
            }
            if (partitioned && StringUtils.isNotEmpty(colName) && !colName.startsWith("#")) {
                partitionCols.add(colName);
            }
        }


        return partitionCols;
    }


    public static void addPartitionsIfNotExists(Connection dbConn, String table, String partition) throws SQLException {
        List<String> partitionCols = getPartitionCols(dbConn, table);
        if(partitionCols == null || partitionCols.size() == 0) {
            return;
        }
        List<String> partitions = showPartitions(dbConn, table);
        for(String part : partitions) {
            if(isSamePartition(part, partition)) {
                return;
            }
        }

        Map<String,String> partMap = partitionMap(partition);
        Set<String> partMapKeySet = partMap.keySet();
        Set<String> partColSet = Sets.newHashSet(partitionCols);
        if(!partMapKeySet.equals(partColSet)) {
            throw new RuntimeException("unmatch partition cols(excpected: +" + partColSet + " but found:" + partMapKeySet +  ")");
        }

        String addPartition = buildAddPartitionStatement(table, partMap);
        System.out.println(addPartition);
        dbConn.createStatement().execute(addPartition);

    }

    private static String buildAddPartitionStatement(String table, Map<String,String> partMap) {
        List<String> partList = new ArrayList<>();
        for(Map.Entry<String,String> entry : partMap.entrySet()) {
            partList.add(quotedString(entry.getKey()) + ASSIGN + SINGLE_QUOTE + entry.getValue() + SINGLE_QUOTE);
        }
        return "alter table " + quotedString(table) + " add partition (" +  StringUtils.join(partList, COMMA) + ")";
    }

    private static boolean isSamePartition(String part1, String part2) {
        Map<String,String> partMap1 = partitionMap(part1);
        Map<String,String> partMap2 = partitionMap(part2);
        return partMap1.equals(partMap2);
    }

    private static Map<String,String> partitionMap(String partition) {
        Map<String,String> map = new HashMap<String,String>();
        String[] keyValues = partition.split(SLASH);
        for(String keyValue : keyValues) {
            String[] split = keyValue.split(ASSIGN);
            map.put(split[0], split[1]);
        }
        return map;
    }

    public static List<String> showPartitions(Connection dbConn, String table) {
        List<String> partitions = new ArrayList<>();
        List<Map<String, Object>> result = DBUtil.executeQuery(dbConn, "show partitions " + quotedString(table));
        Iterator<Map<String, Object>> iter = result.iterator();
        while(iter.hasNext()) {
            Map<String,Object> row = iter.next();
            String dir = (String) row.get("partition");
            partitions.add(dir);
        }
        return partitions;
    }


}

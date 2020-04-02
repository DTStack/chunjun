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


package com.dtstack.flinkx.hive.util;

import com.dtstack.flinkx.hive.TableInfo;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author jiangbo
 * @date 2019/11/29
 */
public class Cdh2HiveMetadataParser extends AbstractHiveMetadataParser {

    @Override
    public void fillTableInfo(TableInfo tableInfo, List<Map<String, Object>> result) {
        Iterator<Map<String, Object>> iter = result.iterator();
        String colName;
        String dataType;
        String comment;
        while (iter.hasNext()) {
            Map<String, Object> row = iter.next();
            colName = (String) row.get("col_name");
            dataType = (String) row.get("data_type");
            comment = (String)row.get("comment");

            if (colName != null && colName.trim().length() > 0) {
                colName = colName.trim();

                if (colName.contains("Location")) {
                    tableInfo.setPath(dataType.trim());
                }

                if (colName.contains("OutputFormat")) {
                    String storedType = getStoredType(dataType.trim());
                    tableInfo.setStore(storedType);
                }
            }

            if(dataType != null && dataType.trim().length() > 0){
                dataType = dataType.trim();

                if(dataType.contains("field.delim")){
                    tableInfo.setDelimiter(comment.trim());
                }
            }
        }
    }
}

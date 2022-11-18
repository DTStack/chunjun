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
package com.dtstack.chunjun.connector.hive.parser;

import com.dtstack.chunjun.connector.hdfs.enums.FileType;
import com.dtstack.chunjun.connector.hive.entity.TableInfo;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AbstractHiveMetadataParser {

    private static final String TEXT_FORMAT = "TextOutputFormat";
    private static final String ORC_FORMAT = "OrcOutputFormat";
    private static final String PARQUET_FORMAT = "MapredParquetOutputFormat";

    public void fillTableInfo(TableInfo tableInfo, List<Map<String, Object>> result) {
        Iterator<Map<String, Object>> iter = result.iterator();
        String colName;
        String dataType;
        while (iter.hasNext()) {
            Map<String, Object> row = iter.next();
            colName = (String) row.get("col_name");
            dataType = (String) row.get("data_type");

            if (colName != null && colName.trim().length() > 0) {
                colName = colName.trim();

                if (colName.contains("Location")) {
                    tableInfo.setPath(dataType.trim());
                }

                if (colName.contains("OutputFormat")) {
                    String storedType = getStoredType(dataType.trim());
                    tableInfo.setStore(storedType);
                }

                if (colName.contains("field.delim")) {
                    tableInfo.setDelimiter(dataType);
                }
            }
        }
    }

    protected String getStoredType(String inputFormatClass) {
        if (inputFormatClass.endsWith(TEXT_FORMAT)) {
            return FileType.TEXT.name();
        } else if (inputFormatClass.endsWith(ORC_FORMAT)) {
            return FileType.ORC.name();
        } else if (inputFormatClass.endsWith(PARQUET_FORMAT)) {
            return FileType.PARQUET.name();
        } else {
            throw new ChunJunRuntimeException("Unsupported fileType:" + inputFormatClass);
        }
    }
}

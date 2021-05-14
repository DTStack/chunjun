/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.solr;

import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.types.Row;
import org.apache.solr.common.SolrDocument;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.List;

public class SolrUtils {


    public static Row convertDocumentToRow(SolrDocument document, List<MetaColumn> metaColumns) {
        Row row = new Row(metaColumns.size());
        int columnIndex = 0;
        for (MetaColumn metaColumn : metaColumns) {
            if (StringUtils.isBlank(metaColumn.getValue())) {
                Object fieldValue = document.getFieldValue(metaColumn.getName());
                if (fieldValue != null) {
                    row.setField(columnIndex, convertValueToAssignType(metaColumn.getType(), String.valueOf(fieldValue), metaColumn.getTimeFormat()));
                } else {
                    row.setField(columnIndex, null);
                }
            } else {
                row.setField(columnIndex, convertValueToAssignType(metaColumn.getType(), metaColumn.getValue(), metaColumn.getTimeFormat()));
            }
            columnIndex++;
        }
        return row;
    }


    private static Object convertValueToAssignType(String columnType, String constantValue, SimpleDateFormat timeFormat) {
        Object column;
        if (org.apache.commons.lang3.StringUtils.isEmpty(constantValue)) {
            return null;
        }

        switch (columnType.toUpperCase()) {
            case "BOOLEAN":
                column = Boolean.valueOf(constantValue);
                break;
            case "SHORT":
                column = Short.valueOf(constantValue);
                break;
            case "INT":
                column = Integer.valueOf(constantValue);
                break;
            case "LONG":
                column = Long.valueOf(constantValue);
                break;
            case "FLOAT":
            case "DOUBLE":
                column = new BigDecimal(constantValue);
                break;
            case "STRING":
                column = constantValue;
                break;
            case "DATE":
                column = DateUtil.stringToDate(constantValue, timeFormat);
                break;
            case "TIMESTAMP":
                column = DateUtil.columnToTimestamp(constantValue, timeFormat);
                break;
            default:
                throw new IllegalArgumentException("Unsupported column type: " + columnType);
        }
        return column;
    }


}

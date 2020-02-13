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

package com.dtstack.flinkx.mongodb;

import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.DateUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.types.Row;
import org.bson.Document;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.dtstack.flinkx.enums.ColumnType.*;

/**
 * Utilities for mongodb database connection and data format conversion
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class MongodbUtil {

    public static Document convertRowToDoc(Row row,List<MetaColumn> columns) throws WriteRecordException {
        Document doc = new Document();
        for (int i = 0; i < columns.size(); i++) {
            MetaColumn column = columns.get(i);
            Object val = convertField(row.getField(i),column);
            if (StringUtils.isNotEmpty(column.getSplitter())){
                val = Arrays.asList(String.valueOf(val).split(column.getSplitter()));
            }

            doc.append(column.getName(),val);
        }

        return doc;
    }

    private static Object convertField(Object val,MetaColumn column){
        ColumnType type = getType(column.getType());
        switch (type) {
            case VARCHAR:
            case VARCHAR2:
            case CHAR:
            case TEXT:
            case STRING:
                if (val instanceof Date){
                    SimpleDateFormat format = DateUtil.getDateTimeFormatter();
                    val = format.format(val);
                }
                break;
            case TIMESTAMP:
                if (!(val instanceof Timestamp)) {
                    val = DateUtil.columnToTimestamp(val, column.getTimeFormat());
                }
                break;
            case DATE:
                if (!(val instanceof Date)) {
                    val = DateUtil.columnToDate(val, column.getTimeFormat());
                }
                break;
            default:
                if(val instanceof BigDecimal){
                    val = ((BigDecimal) val).doubleValue();
                }
                break;
        }

        return val;
    }
}

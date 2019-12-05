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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;


/**
 * Utilities for mongodb database connection and data format conversion
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class MongodbUtil {

    private static final Logger LOG = LoggerFactory.getLogger(MongodbUtil.class);

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
        if(val instanceof BigDecimal){
           val = ((BigDecimal) val).doubleValue();
        }

        if (val instanceof Timestamp && !column.getType().equalsIgnoreCase(ColumnType.INTEGER.name())){
            SimpleDateFormat format = DateUtil.getDateTimeFormatter();
            val= format.format(val);
        }

        return val;
    }
}

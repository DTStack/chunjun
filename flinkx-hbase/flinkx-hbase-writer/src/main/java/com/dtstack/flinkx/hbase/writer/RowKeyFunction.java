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

package com.dtstack.flinkx.hbase.writer;

import com.dtstack.flinkx.hbase.writer.function.FunctionFactory;
import com.dtstack.flinkx.hbase.writer.function.IFunction;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @company: www.dtstack.com
 * @author: toutian
 * @create: 2019/7/23
 */
public class RowKeyFunction {

    private static Logger logger = LoggerFactory.getLogger(RowKeyFunction.class);

    private static Pattern ROWKEY_FIELD_PATTERN = Pattern.compile("\\$\\(.*?\\)");

    private static String LEFT_KUO = "(";

    private static String RIGHT_KUO = ")";

    private static String REGEX_LEFT = "$";

    /**
     * once only support a function
     */
    private IFunction function = null;

    private HbaseOutputFormat format;

    private Map<String, List<String>> rowKeyColumnNamesMap;

    public RowKeyFunction(HbaseOutputFormat format) {
        this.format = format;

        init();
    }

    public byte[] resolve(Row record, String value, ColumnType columnType) {
        String rowKeyValue = value;
        List<String> rowKeyColumnNames = rowKeyColumnNamesMap.get(value);
        if (rowKeyColumnNames != null) {
            for (int i = 0; i < record.getArity(); ++i) {
                String name = format.columnNames.get(i);
                if (!rowKeyColumnNames.contains(name)) {
                    continue;
                }
                rowKeyValue = rowKeyValue.replace("$(" + name + ")", record.getField(i).toString());
            }
        }
        if (function != null) {
//            rowKeyValue = function.eval(rowKeyValue);
        }
        return format.getValueByte(columnType, rowKeyValue);
    }

    private void init() {
        rowKeyColumnNamesMap = new HashMap<>();
//
//        for (int i = 0; i < format.rowkeyColumnTypes.size(); ++i) {
//            Integer index = format.rowkeyColumnIndices.get(i);
//            if (index == null) {
//                String value = format.rowkeyColumnValues.get(i);
//                regalByRules(value);
//            }
//        }
    }

    private void regalByRules(String value) {
        try {
            String rowKeyValue = value;
            if (rowKeyValue.indexOf(LEFT_KUO) >= rowKeyValue.indexOf(RIGHT_KUO)) {
                return;
            }
            String funcStr = StringUtils.substringBefore(rowKeyValue, LEFT_KUO);
            if (!funcStr.endsWith(REGEX_LEFT)) {
                function = FunctionFactory.createFuntion(funcStr);
            }
            if (function != null) {
                rowKeyValue = StringUtils.substring(rowKeyValue, rowKeyValue.indexOf(LEFT_KUO) + 1, rowKeyValue.lastIndexOf(RIGHT_KUO));
            }
            Matcher matcher = ROWKEY_FIELD_PATTERN.matcher(rowKeyValue);
            while (matcher.find()) {
                String fieldKey = matcher.group();
                String key = fieldKey.substring(2, fieldKey.length() - 1);
                if (!format.columnNames.contains(key)) {
                    throw new IllegalArgumentException("please check rowKey:" + key + ", it's not right!");
                }
                List<String> rowKeyColumnNames = rowKeyColumnNamesMap.computeIfAbsent(value, k -> new ArrayList<>());
                rowKeyColumnNames.add(key);
            }
        } catch (Exception e) {
            logger.error("parser value failed", e);
        }
    }

}

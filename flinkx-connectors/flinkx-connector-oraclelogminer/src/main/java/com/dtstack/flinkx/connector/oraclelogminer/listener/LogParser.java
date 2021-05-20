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


package com.dtstack.flinkx.connector.oraclelogminer.listener;

import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.connector.oraclelogminer.conf.LogMinerConf;
import com.dtstack.flinkx.connector.oraclelogminer.entity.LogminerEventRow;
import com.dtstack.flinkx.connector.oraclelogminer.entity.QueueData;
import com.dtstack.flinkx.converter.AbstractCDCRowConverter;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.util.SnowflakeIdWorker;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author jiangbo
 * @date 2020/3/30
 */
public class LogParser {

    public static Logger LOG = LoggerFactory.getLogger(LogParser.class);

    //TO_DATE函数值匹配
    public static Pattern toDatePattern = Pattern.compile("(?i)(?<toDate>(TO_DATE\\('(?<datetime>(.*?))',\\s+'YYYY-MM-DD HH24:MI:SS'\\)))");
    //TO_TIMESTAMP函数值匹配
    public static Pattern timeStampPattern = Pattern.compile("(?i)(?<toTimeStamp>(TO_TIMESTAMP\\('(?<datetime>(.*?))'\\)))");

    public static Pattern timeStampItzPattern = Pattern.compile("(?i)(?<toTimeStampItz>(TO_TIMESTAMP_ITZ\\('(?<datetime>(.*?))'\\)))");

    public static Pattern timeStampTzPattern = Pattern.compile("(?i)(?<toTimeStampTz>(TO_TIMESTAMP_TZ\\('(?<datetime>(.*?))'\\)))");

    public static SnowflakeIdWorker idWorker = new SnowflakeIdWorker(1, 1);

    private LogMinerConf config;

    public LogParser(LogMinerConf config) {
        this.config = config;
    }

    private static String cleanString(String str) {
        if ("NULL".equalsIgnoreCase(str)) {
            return null;
        }

        if (str.startsWith("TIMESTAMP")) {
            str = str.replace("TIMESTAMP ", "");
        }

        if (str.startsWith("'") && str.endsWith("'") && str.length() != 1) {
            str = str.substring(1, str.length() - 1);
        }

        if (str.startsWith("\"") && str.endsWith("\"") && str.length() != 1) {
            str = str.substring(1, str.length() - 1);
        }

        return str.replace("IS NULL", "= NULL").trim();
    }

    private static void parseInsertStmt(Insert insert, ArrayList<LogminerEventRow.Column> beforeData, ArrayList<LogminerEventRow.Column> afterData) {
        ArrayList<String> columnLists = new ArrayList<>();
        for (Column column : insert.getColumns()) {
            columnLists.add(cleanString(column.getColumnName()));
        }

        ExpressionList eList = (ExpressionList) insert.getItemsList();
        List<Expression> valueList = eList.getExpressions();
        int i = 0;
        for (String key : columnLists) {
            String value = cleanString(valueList.get(i).toString());
            afterData.add(new LogminerEventRow.Column(key, value, Objects.isNull(value)));
            beforeData.add(new LogminerEventRow.Column(key, null, true));
            i++;
        }
    }

    private static void parseUpdateStmt(Update update, ArrayList<LogminerEventRow.Column> beforeData, ArrayList<LogminerEventRow.Column> afterData, String sqlRedo) {
        Iterator<Expression> iterator = update.getExpressions().iterator();
        HashSet<String> columns = new HashSet<>(32);
        for (Column c : update.getColumns()) {
            String value = cleanString(iterator.next().toString());
            String columnName = cleanString(c.getColumnName());
            afterData.add(new LogminerEventRow.Column(columnName, value, Objects.isNull(value)));
            columns.add(columnName);
        }

        if (update.getWhere() != null) {
            update.getWhere().accept(new ExpressionVisitorAdapter() {
                @Override
                public void visit(final EqualsTo expr) {
                    String col = cleanString(expr.getLeftExpression().toString());
                    String value = cleanString(expr.getRightExpression().toString());

                    beforeData.add(new LogminerEventRow.Column(col,  value, Objects.isNull(value)));
                    if (!columns.contains(col)) {
                        afterData.add(new LogminerEventRow.Column(col,  value, Objects.isNull(value)));
                    }
                }
            });
        } else {
            LOG.error("where is null when LogParser parse sqlRedo, sqlRedo = {}, update = {}", sqlRedo, update.toString());
        }
    }

    private static void parseDeleteStmt(Delete delete, ArrayList<LogminerEventRow.Column> beforeData, ArrayList<LogminerEventRow.Column> afterData) {
        delete.getWhere().accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(final EqualsTo expr) {
                String col = cleanString(expr.getLeftExpression().toString());
                String value = cleanString(expr.getRightExpression().toString());
                boolean isNull = value.equalsIgnoreCase("= NULL");
                beforeData.add(new LogminerEventRow.Column(col, isNull ? null : value, isNull));
                afterData.add(new LogminerEventRow.Column(col, null, true));
            }
        });
    }

    public LinkedList<RowData> parse(QueueData pair, boolean isOracle10, AbstractCDCRowConverter rowConverter) throws JSQLParserException {
        ColumnRowData logData = (ColumnRowData) pair.getData();

        String schema = logData.getField("schema").asString();
        String tableName = logData.getField("tableName").asString();
        String operation = logData.getField("operation").asString();
        String sqlLog = logData.getField("sqlLog").asString();
        String sqlRedo = sqlLog.replace("IS NULL", "= NULL");
        Timestamp timestamp = logData.getField("opTime").asTimestamp();


        //只有oracle10需要进行toDate toTimestamp转换
        LOG.debug("before parse toDate/toTimestamp sqlRedo = {}", sqlRedo);
        if (isOracle10) {
            sqlRedo = parseToTimeStampTz(parseToTimeStampItz(parseToTimeStamp(parseToDate(sqlRedo))));
        }

        Statement stmt;
        try {
            stmt = CCJSqlParserUtil.parse(sqlRedo);
        } catch (JSQLParserException e) {
            LOG.info("sqlRedo = {}", sqlRedo);
            stmt = CCJSqlParserUtil.parse(sqlRedo.replace("\\'", "\\ '"));
        }

        ArrayList<LogminerEventRow.Column> afterColumnList = new ArrayList<>();
        ArrayList<LogminerEventRow.Column> beforColumnList = new ArrayList<>();

        if (stmt instanceof Insert) {
            parseInsertStmt((Insert) stmt, beforColumnList, afterColumnList);
        } else if (stmt instanceof Update) {
            parseUpdateStmt((Update) stmt, beforColumnList, afterColumnList, sqlRedo);
        } else if (stmt instanceof Delete) {
            parseDeleteStmt((Delete) stmt, beforColumnList, afterColumnList);
        }


        LogminerEventRow logminerEventRow = new LogminerEventRow(beforColumnList, afterColumnList, pair.getScn(), operation, schema, tableName, idWorker.nextId(), timestamp);

        return rowConverter.toInternal(logminerEventRow);
    }

    /**
     * 解析to_date函数
     *
     * @param redoLog
     *
     * @return
     */
    private String parseToDate(String redoLog) {
        Matcher matcher = toDatePattern.matcher(redoLog);
        HashMap<String, String> replaceData = new HashMap<>(8);
        while (matcher.find()) {
            String key = matcher.group("toDate");
            String value = "'" + matcher.group("datetime") + "'";
            replaceData.put(key, value);
        }
        return replace(redoLog, replaceData);
    }


    /**
     * 解析to_timestamp函数
     *
     * @param redoLog
     *
     * @return
     */
    private String parseToTimeStamp(String redoLog) {
        Matcher matcher = timeStampPattern.matcher(redoLog);
        HashMap<String, String> replaceData = new HashMap<>(8);
        while (matcher.find()) {
            String key = matcher.group("toTimeStamp");
            String value = "'" + matcher.group("datetime") + "'";
            replaceData.put(key, value);
        }
        return replace(redoLog, replaceData);
    }

    /**
     * TO_TIMESTAMP_ITZ('2021-05-17 07:08:27.000000')格式解析
     * @param redoLog
     * @return
     */
    private String parseToTimeStampItz(String redoLog) {
        Matcher matcher = timeStampItzPattern.matcher(redoLog);
        HashMap<String, String> replaceData = new HashMap<>(8);
        while (matcher.find()) {
            String key = matcher.group("toTimeStampItz");
            String value = "'" + matcher.group("datetime") + "'";
            replaceData.put(key, value);
        }
        return replace(redoLog, replaceData);
    }


    /**
     * TO_TIMESTAMP_TZ('2021-05-17 07:08:27.000000 上午 +08:00')
     * @param redoLog
     * @return
     */
    private String parseToTimeStampTz(String redoLog) {
        Matcher matcher = timeStampTzPattern.matcher(redoLog);
        HashMap<String, String> replaceData = new HashMap<>(8);
        while (matcher.find()) {
            String key = matcher.group("toTimeStampTz");
            String value = "'" + matcher.group("datetime") + "'";
            replaceData.put(key, value);
        }
        return replace(redoLog, replaceData);
    }

    private String replace(String redoLog, HashMap<String, String> replaceData) {
        if (MapUtils.isNotEmpty(replaceData)) {
            for (Map.Entry<String, String> entry : replaceData.entrySet()) {
                //to_timeStamp/to_date()函数有括号 需要转义
                String k = entry.getKey().replaceAll("\\(", "\\\\(").replaceAll("\\)", "\\\\)");
                String v = entry.getValue();
                redoLog = redoLog.replaceAll(k, v);
            }
        }
        return redoLog;
    }

}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.clickhouse.reader;

import com.alibaba.fastjson.JSON;
import com.dtstack.flinkx.clickhouse.core.ClickhouseConfig;
import com.dtstack.flinkx.clickhouse.core.ClickhouseUtil;
import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.JsonUtils;
import com.google.gson.Gson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.apache.hadoop.hdfs.web.JsonUtil;
import ru.yandex.clickhouse.domain.ClickHouseDataType;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Date: 2019/11/05
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class ClickhouseInputFormat extends RichInputFormat {
    protected static final long serialVersionUID = 1L;

    protected ClickhouseConfig config;
    protected transient Connection conn;
    protected transient Statement statement;
    protected transient ResultSet resultSet;
    protected boolean hasNext;
    protected int columnCount;

    private Row lastRow = null;

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        try {
            LOG.info("inputSplit = {}", inputSplit);
            conn = ClickhouseUtil.getConnection(config.getUrl(), config.getUsername(), config.getPassword(), config.getClickhouseProp());
            Statement statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            statement.setFetchSize(Integer.MIN_VALUE);
            statement.setQueryTimeout(config.getQueryTimeOut());
            String querySql = buildQuerySql(inputSplit);
            resultSet = statement.executeQuery(querySql);
            columnCount = resultSet.getMetaData().getColumnCount();
            hasNext = resultSet.next();
        } catch (Exception e) {
            LOG.error("open failed,e = {}", ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(e);
        }

        LOG.info("JdbcInputFormat[{}]open: end", jobName);
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        if (!hasNext) {
            return null;
        }
        row = new Row(columnCount);
        try {
            for (int i = 0; i < columnCount; i++) {
                String type = config.getColumn().get(i).getType();
                if(StringUtils.isBlank(type)){
                    type = "String";
                }
                ClickHouseDataType dataType = ClickHouseDataType.fromTypeString(type);
                row.setField(i, resultSet.getObject(i + 1, dataType.getJavaClass()));
            }
        } catch (Exception e) {
            LOG.error("Couldn't read data, e = {}", ExceptionUtil.getErrorMessage(e));
            throw new IOException(e);
        }

        return row;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        InputSplit[] splits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return splits;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    @Override
    protected void closeInternal() throws IOException {
        ClickhouseUtil.closeDBResources(resultSet, statement, conn);
    }

    protected String buildQuerySql(InputSplit inputSplit)  throws SQLException {
        StringBuilder sql = new StringBuilder(64);
        sql.append(" select ");
        List<MetaColumn> column = config.getColumn();
        if (CollectionUtils.isEmpty(column)) {
            try {
                ResultSet rs = conn.createStatement().executeQuery("desc " + config.getTable());
                Map<String, String> map = new LinkedHashMap<>();
                int i = 0;
                while (rs.next()){
                    MetaColumn metaColumn = new MetaColumn();
                    metaColumn.setIndex(i);
                    metaColumn.setName(rs.getString(1));
                    metaColumn.setType(rs.getString(2));
                    column.add(metaColumn);
                    i++;
                }
            }catch (SQLException e){
                LOG.error("error to get {} schema, e = {}", config.getTable(), ExceptionUtil.getErrorMessage(e));
                throw e;
            }
            LOG.info("init column, column = {}", JSON.toJSONString(column));
        }
        sql.append("(");
        for (MetaColumn metaColumn : column) {
            sql.append(metaColumn.getName()).append(", ");
        }
        sql.deleteCharAt(sql.lastIndexOf(",")).append(") ");
        sql.append(" from ").append(config.getTable());
        String filter = config.getFilter();
        if (StringUtils.isNotBlank(filter)) {
            sql.append(" where ").append(filter);
        }
        String querySql = sql.toString();
        LOG.info("Executing sql is: {}", querySql);
        return querySql;
    }
}

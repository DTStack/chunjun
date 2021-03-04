/*
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

package com.dtstack.metadata.rdb.inputformat;

import com.dtstack.flinkx.metadata.core.entity.MetadataEntity;
import com.dtstack.flinkx.metadata.inputformat.MetadataBaseInputFormat;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.metadata.rdb.core.entity.ColumnEntity;
import com.dtstack.metadata.rdb.core.entity.ConnectionInfo;
import com.dtstack.metadata.rdb.core.util.MetadataDbUtil;
import com.dtstack.metadata.rdb.core.entity.MetadatardbEntity;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.dtstack.metadata.rdb.core.constants.RdbCons.KEY_FALSE;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.KEY_TES;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.KEY_TRUE;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.RESULT_COLUMN_DEF;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.RESULT_COLUMN_NAME;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.RESULT_COLUMN_SIZE;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.RESULT_DECIMAL_DIGITS;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.RESULT_IS_NULLABLE;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.RESULT_ORDINAL_POSITION;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.RESULT_REMARKS;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.RESULT_TABLE_NAME;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.RESULT_TYPE_NAME;

/**
 * @author kunni@dtstack.com
 */

abstract public class MetadatardbInputFormat extends MetadataBaseInputFormat {

    /**连接对象*/
    protected Connection connection;

    /**connection初始化的statement*/
    protected Statement statement;

    /**连接信息*/
    public ConnectionInfo connectionInfo;

    @Override
    protected void doOpenInternal() {
        try {
            if (connection == null) {
                connection = getConnection();
                statement = connection.createStatement();
            }
            switchDataBase();
            if (CollectionUtils.isEmpty(tableList)) {
                tableList = showTables();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * 设置当前数据库环境
     *
     * @throws SQLException sql异常
     */

    public void switchDataBase() throws SQLException {

    }

    @Override
    protected void closeInternal() {

    }

    /**
     * 当传入表名为空时，手动查询所有表
     * 提供默认实现为只查询表名的情况，查询
     *
     * @return 表名
     */
    public List<Object> showTables() throws SQLException {
        List<Object> tables = new ArrayList<>();
        try (ResultSet resultSet = connection.getMetaData().getTables(currentDatabase, null, null, null)) {
            while (resultSet.next()) {
                tables.add(resultSet.getString(RESULT_TABLE_NAME));
            }
        } catch (SQLException e) {
            LOG.error("failed to query table, currentDb = {} ", currentDatabase);
            throw new SQLException("show tables error" + e.getMessage(), e);
        }
        return tables;
    }

    @Override
    public MetadataEntity createMetadataEntity() throws Exception {
        MetadatardbEntity entity = createMetadatardbEntity();
        entity.setSchema(currentDatabase);
        entity.setTableName((String) currentObject);
        return entity;
    }

    /**
     * 元数据信息
     *
     * @return MetadatardbEntity
     * @throws IOException sql异常
     */
    abstract public MetadatardbEntity createMetadatardbEntity() throws Exception;


    /**
     * 查询字段信息
     *
     * @return
     * @throws SQLException
     */
    public List<? extends ColumnEntity> queryColumn(String schema) throws SQLException {
        List<ColumnEntity> columnEntities = new ArrayList<>();
        String currentTable = (String) currentObject;
        try (ResultSet resultSet = connection.getMetaData().getColumns(currentDatabase, schema, currentTable, null);
             ResultSet primaryResultSet = connection.getMetaData().getPrimaryKeys(currentDatabase, schema, currentTable)) {
            Set<String> primaryColumns = Sets.newHashSet();
            while (primaryResultSet.next()) {
                primaryColumns.add(primaryResultSet.getString(RESULT_COLUMN_NAME));
            }
            while (resultSet.next()) {
                ColumnEntity columnEntity = new ColumnEntity();
                columnEntity.setName(resultSet.getString(RESULT_COLUMN_NAME));
                columnEntity.setType(resultSet.getString(RESULT_TYPE_NAME));
                columnEntity.setIndex(resultSet.getInt(RESULT_ORDINAL_POSITION));
                columnEntity.setDefaultValue(resultSet.getString(RESULT_COLUMN_DEF));
                columnEntity.setNullAble(StringUtils.equals(resultSet.getString(RESULT_IS_NULLABLE), KEY_TES) ? KEY_TRUE : KEY_FALSE);
                columnEntity.setComment(resultSet.getString(RESULT_REMARKS));
                columnEntity.setDigital(resultSet.getInt(RESULT_DECIMAL_DIGITS));
                columnEntity.setLength(resultSet.getInt(RESULT_COLUMN_SIZE));
                columnEntity.setPrimaryKey(primaryColumns.contains(columnEntity.getName()) ? KEY_TRUE : KEY_FALSE);
                columnEntities.add(columnEntity);
            }
        } catch (SQLException e) {
            LOG.error("queryColumn failed, cause: {} ", ExceptionUtil.getErrorMessage(e));
            throw e;
        }
        return columnEntities;
    }

    @Override
    public void closeInputFormat() throws IOException {
        closeResource();
        super.closeInputFormat();
    }

    /**
     * jdbc数据源获取连接
     *
     * @return
     * @throws SQLException
     */
    public Connection getConnection() throws SQLException {
        return MetadataDbUtil.getConnection(connectionInfo);
    }

    /**
     * 关闭资源
     *
     * @throws IOException
     */
    public void closeResource() throws IOException {
        try {
            MetadataDbUtil.close(statement, connection);
        } catch (Exception e) {
            throw new IOException("close resource error" + e.getMessage(), e);
        }
    }

    /**
     * 通过jdbc执行sql
     *
     * @param sql
     * @param statement
     * @return
     */
    protected ResultSet executeQuery0(String sql, Statement statement) {
        ResultSet resultSet = null;
        if (StringUtils.isNotBlank(sql)) {
            LOG.info("execute SQL : {}", sql);
            try {
                if (statement != null) {
                    resultSet = statement.executeQuery(sql);
                }
            } catch (SQLException e) {
                LOG.error("execute SQL failed : {}", ExceptionUtil.getErrorMessage(e));
            }

        }
        return resultSet;
    }
}

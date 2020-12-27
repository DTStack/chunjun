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

import com.dtstack.flinkx.metadata.entity.MetadataEntity;
import com.dtstack.flinkx.metadata.inputformat.MetadataBaseInputFormat;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.metadata.rdb.core.util.MetadataDbUtil;
import com.dtstack.metadata.rdb.entity.ColumnEntity;
import com.dtstack.metadata.rdb.entity.MetadatardbEntity;
import com.dtstack.metadata.rdb.entity.TableEntity;
import org.apache.commons.collections.CollectionUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

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

    protected Connection connection;

    protected Statement statement;

    protected String currentTable;

    protected String driverName;

    protected String url;

    protected String username;

    protected String password;

    @Override
    protected void initJob() {
        try{
            Class.forName(driverName);
            connection = MetadataDbUtil.getConnection(url, username, password);
            statement = connection.createStatement();
            switchDataBase();
        }catch (ClassNotFoundException | SQLException e){
            throw new RuntimeException(e);
        }
        if(CollectionUtils.isEmpty(tableList)){
            tableList = showTables();
        }
    }

    /**
     * 设置当前数据库环境
     * @throws SQLException sql异常
     */

    abstract public void switchDataBase() throws SQLException;

    @Override
    protected void closeInternal() {
        MetadataDbUtil.closeConnection(connection);
    }

    /**
     * 当传入表名为空时，手动查询所有表
     * 提供默认实现为只查询表名的情况，查询
     * @return 表名
     */
    public List<Object> showTables() {
        List<Object> tables = new ArrayList<>();
        try(ResultSet resultSet = connection.getMetaData().getTables(currentDatabase, null, null, null)){
               while (resultSet.next()){
                   tables.add(resultSet.getString(RESULT_TABLE_NAME));
               }
        }catch (SQLException e){
            LOG.error("failed to query table, currentDb = {} ", currentDatabase);
            return tables;
        }
        return tables;
    }

    @Override
    public MetadataEntity createMetadataEntity() throws IOException {
        currentTable = (String) currentObject;
        MetadatardbEntity entity = createMetadatardbEntity();
        entity.setTableProperties(createTableEntity());
        entity.setColumn(queryColumn());
        return entity;
    }

    /**
     * 元数据信息
     * @return MetadatardbEntity
     * @throws IOException sql异常
     */
    abstract public MetadatardbEntity createMetadatardbEntity() throws IOException;

    /**
     * 表的元数据属性
     * @return TableEntity
     * @throws IOException sql异常
     */
    abstract public TableEntity createTableEntity()  throws IOException;

    public List<ColumnEntity> queryColumn()  throws IOException {
        List<ColumnEntity> columnEntities = new ArrayList<>();
        try(ResultSet resultSet = connection.getMetaData().getColumns(currentDatabase, null, currentTable, null)){
            while (resultSet.next()){
                ColumnEntity columnEntity = new ColumnEntity();
                columnEntity.setName(resultSet.getString(RESULT_COLUMN_NAME));
                columnEntity.setType(resultSet.getString(RESULT_TYPE_NAME));
                columnEntity.setPosition(resultSet.getString(RESULT_ORDINAL_POSITION));
                columnEntity.setDefaultValue(resultSet.getString(RESULT_COLUMN_DEF));
                columnEntity.setNullAble(resultSet.getString(RESULT_IS_NULLABLE));
                columnEntity.setComment(resultSet.getString(RESULT_REMARKS));
                columnEntity.setDigital(resultSet.getString(RESULT_DECIMAL_DIGITS));
                columnEntity.setLength(resultSet.getString(RESULT_COLUMN_SIZE));
                columnEntities.add(columnEntity);
            }
        }catch (SQLException e){
            LOG.error("queryColumn failed, cause: {} ", ExceptionUtil.getErrorMessage(e));
        }
        return columnEntities;
    }

    public void setUsername(String username){
        this.username = username;
    }

    public void setPassword(String password){
        this.password = password;
    }

    public void setUrl(String url){
        this.url = url;
    }

    public void setDriverName(String driverName){
        this.driverName = driverName;
    }

}

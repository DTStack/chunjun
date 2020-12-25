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
import com.dtstack.flinkx.metadata.inputformat.MetadataBaseInputSplit;
import com.dtstack.metadata.rdb.core.util.MetadataDbUtil;
import com.dtstack.metadata.rdb.entity.ColumnEntity;
import com.dtstack.metadata.rdb.entity.MetadatardbEntity;
import com.dtstack.metadata.rdb.entity.TableEntity;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.core.io.InputSplit;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.dtstack.metadata.rdb.core.constants.RdbConstants.RESULT_TABLE_NAME;

/**
 * @author kunni@dtstack.com
 */

abstract public class MetadatardbInputFormat extends MetadataBaseInputFormat {

    protected Connection connection;

    protected Statement statement;

    protected String currentDb;

    protected List<Object> tableList;

    protected String driverName;

    protected String url;

    protected String username;

    protected String password;



    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        LOG.info("inputSplit : {}", inputSplit);
        try{
            Class.forName(driverName);
            connection = MetadataDbUtil.getConnection(url, username, password);
            statement = connection.createStatement();
        }catch (ClassNotFoundException | SQLException e){
            throw new RuntimeException(e);
        }

        tableList = ((MetadataBaseInputSplit) inputSplit).getTableList();
        currentDb = ((MetadataBaseInputSplit) inputSplit).getDbName();

        if(CollectionUtils.isEmpty(tableList)){
            tableList = showTables();
        }
        iterator = tableList.iterator();
    }

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
        try(ResultSet resultSet = connection.getMetaData().getTables(null, currentDb, null, null)){
               while (resultSet.next()){
                   tables.add(resultSet.getString(RESULT_TABLE_NAME));
               }
        }catch (SQLException e){
            LOG.error("failed to query table, currentDb = {} ", currentDb);
            return tables;
        }
        return tables;
    }

    @Override
    public MetadataEntity createMetadataEntity() {
        MetadatardbEntity entity = new MetadatardbEntity();
        entity.setTableProperties(queryTableEntity());
        entity.setColumn(queryColumn());
        return entity;
    }

    public TableEntity queryTableEntity(){
        TableEntity tableEntity = new TableEntity();
        return tableEntity;
    }

    public List<ColumnEntity> queryColumn(){
        List<ColumnEntity> columnEntities = new ArrayList<>();
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

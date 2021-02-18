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

package com.dtstack.flinkx.metadatamysql.inputformat;

import com.dtstack.flinkx.metadatamysql.entity.IndexEntity;
import com.dtstack.flinkx.metadatamysql.entity.MetadataMysqlEntity;
import com.dtstack.flinkx.metadatamysql.entity.MysqlTableEntity;
import com.dtstack.metadata.rdb.core.entity.MetadatardbEntity;

import com.dtstack.metadata.rdb.core.entity.TableEntity;
import com.dtstack.metadata.rdb.inputformat.MetadatardbInputFormat;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import static com.dtstack.flinkx.metadatamysql.constants.MysqlMetadataCons.RESULT_COLUMN_NAME;
import static com.dtstack.flinkx.metadatamysql.constants.MysqlMetadataCons.RESULT_CREATE_TIME;
import static com.dtstack.flinkx.metadatamysql.constants.MysqlMetadataCons.RESULT_DATA_LENGTH;
import static com.dtstack.flinkx.metadatamysql.constants.MysqlMetadataCons.RESULT_ENGINE;
import static com.dtstack.flinkx.metadatamysql.constants.MysqlMetadataCons.RESULT_INDEX_COMMENT;
import static com.dtstack.flinkx.metadatamysql.constants.MysqlMetadataCons.RESULT_INDEX_TYPE;
import static com.dtstack.flinkx.metadatamysql.constants.MysqlMetadataCons.RESULT_KEY_NAME;
import static com.dtstack.flinkx.metadatamysql.constants.MysqlMetadataCons.RESULT_ROWS;
import static com.dtstack.flinkx.metadatamysql.constants.MysqlMetadataCons.RESULT_ROW_FORMAT;
import static com.dtstack.flinkx.metadatamysql.constants.MysqlMetadataCons.RESULT_TABLE_COMMENT;
import static com.dtstack.flinkx.metadatamysql.constants.MysqlMetadataCons.RESULT_TABLE_TYPE;
import static com.dtstack.flinkx.metadatamysql.constants.MysqlMetadataCons.SQL_QUERY_INDEX;
import static com.dtstack.flinkx.metadatamysql.constants.MysqlMetadataCons.SQL_QUERY_TABLE_INFO;
import static com.dtstack.flinkx.metadatamysql.constants.MysqlMetadataCons.SQL_SWITCH_DATABASE;

/**
 * @author : kunni@dtstack.com
 */

public class MetadatamysqlInputFormat extends MetadatardbInputFormat {

    private static final long serialVersionUID = 1L;

    protected List<IndexEntity> queryIndex() throws SQLException {
        List<IndexEntity> indexEntities = new LinkedList<>();
        String sql = String.format(SQL_QUERY_INDEX, currentObject);
        ResultSet rs = statement.executeQuery(sql);
        while (rs.next()) {
            IndexEntity entity = new IndexEntity();
            entity.setIndexName(rs.getString(RESULT_KEY_NAME));
            entity.setColumnName(rs.getString(RESULT_COLUMN_NAME));
            entity.setIndexType(rs.getString(RESULT_INDEX_TYPE));
            entity.setIndexComment(rs.getString(RESULT_INDEX_COMMENT));
            indexEntities.add(entity);
        }
        rs.close();
        return indexEntities;
    }

    @Override
    public void switchDataBase() throws SQLException {
        statement.execute(String.format(SQL_SWITCH_DATABASE, currentDatabase));
    }

    @Override
    public MetadatardbEntity createMetadatardbEntity() throws IOException {
        MetadataMysqlEntity metadataMysqlEntity = new MetadataMysqlEntity();
        try {
            metadataMysqlEntity.setIndexEntities(queryIndex());
            metadataMysqlEntity.setTableProperties(createTableEntity());
            metadataMysqlEntity.setColumns(queryColumn());
        } catch (Exception e) {
            throw new IOException(e);
        }
        return metadataMysqlEntity;
    }

    public TableEntity createTableEntity() throws IOException {
        MysqlTableEntity entity = new MysqlTableEntity();
        String sql = String.format(SQL_QUERY_TABLE_INFO, currentDatabase, currentObject);
        try (ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                entity.setTableType(RESULT_TABLE_TYPE);
                entity.setComment(rs.getString(RESULT_TABLE_COMMENT));
                entity.setCreateTime(rs.getString(RESULT_CREATE_TIME));
                entity.setTotalSize(rs.getLong(RESULT_DATA_LENGTH));
                entity.setRows(rs.getLong(RESULT_ROWS));
                entity.setEngine(rs.getString(RESULT_ENGINE));
                entity.setRowFormat(rs.getString(RESULT_ROW_FORMAT));
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
        return entity;
    }

}

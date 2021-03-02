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

package com.dtstack.flinkx.metadatavertica.inputformat;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.metadatavertica.entity.MetadataVerticaEntity;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.metadata.rdb.core.entity.ColumnEntity;
import com.dtstack.metadata.rdb.core.entity.MetadatardbEntity;
import com.dtstack.metadata.rdb.core.entity.TableEntity;
import com.dtstack.metadata.rdb.inputformat.MetadatardbInputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.core.io.InputSplit;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


import static com.dtstack.flinkx.metadatavertica.constants.VerticaMetaDataCons.RESULT_SET_COLUMN_DEF;
import static com.dtstack.flinkx.metadatavertica.constants.VerticaMetaDataCons.RESULT_SET_COLUMN_NAME;
import static com.dtstack.flinkx.metadatavertica.constants.VerticaMetaDataCons.RESULT_SET_COLUMN_SIZE;
import static com.dtstack.flinkx.metadatavertica.constants.VerticaMetaDataCons.RESULT_SET_DECIMAL_DIGITS;
import static com.dtstack.flinkx.metadatavertica.constants.VerticaMetaDataCons.RESULT_SET_IS_NULLABLE;
import static com.dtstack.flinkx.metadatavertica.constants.VerticaMetaDataCons.RESULT_SET_ORDINAL_POSITION;
import static com.dtstack.flinkx.metadatavertica.constants.VerticaMetaDataCons.RESULT_SET_REMARKS;
import static com.dtstack.flinkx.metadatavertica.constants.VerticaMetaDataCons.RESULT_SET_TABLE_NAME;
import static com.dtstack.flinkx.metadatavertica.constants.VerticaMetaDataCons.RESULT_SET_TYPE_NAME;
import static com.dtstack.flinkx.metadatavertica.constants.VerticaMetaDataCons.SQL_COMMENT;
import static com.dtstack.flinkx.metadatavertica.constants.VerticaMetaDataCons.SQL_CREATE_TIME;
import static com.dtstack.flinkx.metadatavertica.constants.VerticaMetaDataCons.SQL_PT_COLUMN;
import static com.dtstack.flinkx.metadatavertica.constants.VerticaMetaDataCons.SQL_TOTAL_SIZE;

/** 读取vertica的元数据
 * @author kunni@dtstack.com
 */
public class MetadataverticaInputFormat extends MetadatardbInputFormat {

    protected static final long serialVersionUID = 1L;

    /**创建时间集合*/
    protected Map<String, String> createTimeMap;

    /**表的描述集合*/
    protected Map<String, String> commentMap;

    /**表大小集合*/
    protected Map<String, String> totalSizeMap;

    /**分区字段集合*/
    protected Map<String, String> ptColumnMap;

    List<ColumnEntity> ptColumns = new LinkedList<>();

    /**
     * 采用数组是为了构建Varchar(10)、Decimal(10,2)这种格式
     */
    private static final List<String> SINGLE_DIGITAL_TYPE = Arrays.asList("Integer", "Varchar", "Char", "Numeric");

    private static final List<String> DOUBLE_DIGITAL_TYPE = Arrays.asList("Timestamp", "Decimal");


    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        super.openInternal(inputSplit);
        init();
    }

    @Override
    public MetadatardbEntity createMetadatardbEntity() throws Exception {
        return queryMetaData((String) currentObject);
    }

    @Override
    public List<Object> showTables() {
        List<Object> tables = new LinkedList<>();
        try (ResultSet resultSet = connection.getMetaData().getTables(null, currentDatabase, null, null)) {
            while (resultSet.next()) {
                tables.add(resultSet.getString(RESULT_SET_TABLE_NAME));
            }
        } catch (SQLException e) {
            LOG.error("query table lists failed, {}", ExceptionUtil.getErrorMessage(e));
        }
        return tables;
    }

    protected MetadataVerticaEntity queryMetaData(String tableName) {
        MetadataVerticaEntity metadataVerticaEntity = new MetadataVerticaEntity();
        metadataVerticaEntity.setPartitionColumns(ptColumns);
        ptColumns.clear();
        metadataVerticaEntity.setTableProperties(queryTableProp(tableName));
        metadataVerticaEntity.setColumns(queryColumn(tableName));
        return metadataVerticaEntity;
    }

    protected String quote(String name) {
        return name;
    }

    protected void init() {
        queryCreateTime();
        queryComment();
        queryTotalSizeMap();
        queryPtColumnMap();
    }

    @Override
    public List<ColumnEntity> queryColumn(String schema) {
        List<ColumnEntity> columns = new LinkedList<>();
        String tableName = (String) currentObject;
        try (ResultSet resultSet = connection.getMetaData().getColumns(null, currentDatabase, tableName, null)) {
            while (resultSet.next()) {
                ColumnEntity verticaColumnEntity = new ColumnEntity();
                String columnName = resultSet.getString(RESULT_SET_COLUMN_NAME);
                verticaColumnEntity.setName(columnName);
                String dataSize = resultSet.getString(RESULT_SET_COLUMN_SIZE);
                String digits = resultSet.getString(RESULT_SET_DECIMAL_DIGITS);
                String type = resultSet.getString(RESULT_SET_TYPE_NAME);
                if (SINGLE_DIGITAL_TYPE.contains(type)) {
                    type += ConstantValue.LEFT_PARENTHESIS_SYMBOL + dataSize + ConstantValue.RIGHT_PARENTHESIS_SYMBOL;
                } else if (DOUBLE_DIGITAL_TYPE.contains(type)) {
                    type += ConstantValue.LEFT_PARENTHESIS_SYMBOL + dataSize + ConstantValue.COMMA_SYMBOL + digits + ConstantValue.RIGHT_PARENTHESIS_SYMBOL;
                }
                verticaColumnEntity.setType(type);
                verticaColumnEntity.setComment(resultSet.getString(RESULT_SET_REMARKS));
                verticaColumnEntity.setIndex(resultSet.getInt(RESULT_SET_ORDINAL_POSITION));
                verticaColumnEntity.setNullAble(resultSet.getString(RESULT_SET_IS_NULLABLE));
                verticaColumnEntity.setDefaultValue(resultSet.getString(RESULT_SET_COLUMN_DEF));
                // 分区列信息,vertical partition express 中字段自动增加表名
                String expressColumn = tableName + ConstantValue.POINT_SYMBOL + columnName;
                String partitionExpression = ptColumnMap.get(tableName);
                if (StringUtils.isNotBlank(partitionExpression) && partitionExpression.contains(expressColumn)) {
                    ptColumns.add(verticaColumnEntity);
                } else {
                    columns.add(verticaColumnEntity);
                }
            }
        } catch (SQLException e) {
            LOG.error("query columns failed, {}", ExceptionUtil.getErrorMessage(e));
        }
        return columns;
    }

    /**
     * 获取表级别的元数据信息
     *
     * @param tableName 表名
     * @return 表的元数据
     */
    public TableEntity queryTableProp(String tableName) {
        TableEntity verticaTableEntity = new TableEntity();
        verticaTableEntity.setCreateTime(createTimeMap.get(tableName));
        verticaTableEntity.setComment(commentMap.get(tableName));
        // 单位 byte
        String totalSize = totalSizeMap.get(tableName);
        totalSize = totalSize == null ? "0" : totalSize;
        verticaTableEntity.setTotalSize(Long.valueOf(totalSize));
        return verticaTableEntity;
    }

    /**
     * 获取创建时间
     */
    public void queryCreateTime() {
        createTimeMap = new HashMap<>(16);
        String sql = String.format(SQL_CREATE_TIME, currentDatabase);
        try (ResultSet resultSet = executeQuery0(sql, statement)) {
            while (resultSet.next()) {
                createTimeMap.put(resultSet.getString(1), resultSet.getString(2));
            }
        } catch (SQLException e) {
            LOG.error("query create time failed, {}", ExceptionUtil.getErrorMessage(e));
        }
    }

    /**
     * 获取表注释
     */
    public void queryComment() {
        commentMap = new HashMap<>(16);
        String sql = String.format(SQL_COMMENT, currentDatabase);
        try (ResultSet resultSet = executeQuery0(sql, statement)) {
            while (resultSet.next()) {
                commentMap.put(resultSet.getString(1), resultSet.getString(2));
            }
        } catch (SQLException e) {
            LOG.error("query comment failed, {}", ExceptionUtil.getErrorMessage(e));
        }
    }

    /**
     * 获取表的总大小
     */
    public void queryTotalSizeMap() {
        totalSizeMap = new HashMap<>(16);
        String sql = String.format(SQL_TOTAL_SIZE, currentDatabase);
        try (ResultSet resultSet = executeQuery0(sql, statement)) {
            while (resultSet.next()) {
                totalSizeMap.put(resultSet.getString(1), resultSet.getString(2));
            }
        } catch (SQLException e) {
            LOG.error("query totalSize failed, {}", ExceptionUtil.getErrorMessage(e));
        }
    }

    /**
     * 获取分区列信息
     */
    public void queryPtColumnMap() {
        ptColumnMap = new HashMap<>(16);
        String sql = String.format(SQL_PT_COLUMN, currentDatabase);
        try (ResultSet resultSet = executeQuery0(sql, statement)) {
            while (resultSet.next()) {
                String expression = resultSet.getString(2);
                ptColumnMap.put(resultSet.getString(1), expression);
            }
        } catch (SQLException e) {
            LOG.error("query partition columns failed, {}", ExceptionUtil.getErrorMessage(e));
        }
    }

}

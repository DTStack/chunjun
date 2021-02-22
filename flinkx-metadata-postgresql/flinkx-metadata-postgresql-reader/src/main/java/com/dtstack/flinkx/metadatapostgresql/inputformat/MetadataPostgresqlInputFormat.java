package com.dtstack.flinkx.metadatapostgresql.inputformat;


import com.dtstack.flinkx.metadatapostgresql.constants.PostgresqlCons;
import com.dtstack.flinkx.metadatapostgresql.entity.MetadataPostgresqlEntity;
import com.dtstack.flinkx.metadatapostgresql.entity.IndexMetaData;
import com.dtstack.flinkx.metadatapostgresql.entity.TableMetaData;
import com.dtstack.flinkx.metadatapostgresql.utils.CommonUtils;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.metadata.rdb.core.entity.ColumnEntity;
import com.dtstack.metadata.rdb.core.entity.MetadatardbEntity;
import com.dtstack.metadata.rdb.core.util.MetadataDbUtil;
import com.dtstack.metadata.rdb.inputformat.MetadatardbInputFormat;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.metadata.core.util.BaseCons.DEFAULT_OPERA_TYPE;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.RESULT_COLUMN_DEF;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.RESULT_COLUMN_NAME;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.RESULT_COLUMN_SIZE;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.RESULT_DECIMAL_DIGITS;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.RESULT_IS_NULLABLE;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.RESULT_ORDINAL_POSITION;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.RESULT_REMARKS;
import static com.dtstack.metadata.rdb.core.constants.RdbCons.RESULT_TYPE_NAME;


/**
 *
 * @author shitou
 * @date 2020/12/9 16:25
 */
public class MetadataPostgresqlInputFormat extends MetadatardbInputFormat {


    /**
     * 是否设置了搜索路径
     */
    private boolean isChanged = false;

    /**
     * schema
     */
    private String schemaName;

    /**
     * table
     */
    private String tableName;



    @Override
    protected void doOpenInternal(){
        try {
            connection = getConnectionForCurrentDataBase();
            statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            if (CollectionUtils.isEmpty(tableList)) {
                tableList = showTables();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Row nextRecordInternal(Row row) {
        MetadataPostgresqlEntity metadataPostgresqlEntity = new MetadataPostgresqlEntity();
        currentObject = iterator.next();
        Map<String,String> map = (Map<String,String>) currentObject;
        //保证在同一schema下只需要设置一次搜索路径
        if(schemaName != null && !map.get(PostgresqlCons.KEY_SCHEMA_NAME).equals(schemaName)){
            isChanged = false;
        }
        schemaName = map.get(PostgresqlCons.KEY_SCHEMA_NAME);
        tableName = map.get(PostgresqlCons.KEY_TABLE_NAME);

        try {
            metadataPostgresqlEntity = (MetadataPostgresqlEntity) createMetadatardbEntity();
            metadataPostgresqlEntity.setDataBaseName(currentDatabase);
            metadataPostgresqlEntity.setSchema(schemaName);
            metadataPostgresqlEntity.setTableName(tableName);
            metadataPostgresqlEntity.setQuerySuccess(true);
        } catch (Exception e) {
            metadataPostgresqlEntity.setQuerySuccess(false);
            metadataPostgresqlEntity.setErrorMsg(ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(e);
        }
        metadataPostgresqlEntity.setOperaType(DEFAULT_OPERA_TYPE);
        return Row.of(GsonUtil.GSON.toJson(metadataPostgresqlEntity));
    }


    @Override
    public MetadatardbEntity createMetadatardbEntity() throws Exception {
        MetadataPostgresqlEntity postgresqlEntity = new MetadataPostgresqlEntity();
        postgresqlEntity.setColumns(queryColumn(schemaName));
        TableMetaData tableMetaData = new TableMetaData();
        tableMetaData.setTableName(tableName);
        tableMetaData.setRows(showTableDataCount());
        tableMetaData.setIndexes(showIndexes());
        tableMetaData.setPrimaryKey(showTablePrimaryKey());
        tableMetaData.setTotalSize(showTableSize());
        postgresqlEntity.setTableProperties(tableMetaData);
        return postgresqlEntity;
    }

    /**
     *  postgresql没有对应的切换database的sql语句，所以此方法暂不实现
     **/
    @Override
    public void switchDataBase() throws SQLException {

    }

    /**
     * 查询当前database中所有表名
     * @return List<Object>
     **/
    @Override
    public List<Object> showTables() throws SQLException {
        List<Object> tables = new ArrayList<>();
        try (ResultSet resultSet = statement.executeQuery(PostgresqlCons.SQL_SHOW_TABLES)) {
            while (resultSet.next()) {
                HashMap<String, String> map = new HashMap<>();
                map.put(PostgresqlCons.KEY_SCHEMA_NAME,resultSet.getString("table_schema"));
                map.put(PostgresqlCons.KEY_TABLE_NAME, resultSet.getString("table_name"));
                tables.add(map);
            }
        } catch (SQLException e) {
            LOG.error("failed to query table, currentDb = {} ", currentDatabase);
            throw new SQLException("show tables error"+e.getMessage(),e);
        }
        return tables;
    }

    /**
     *查询表所占磁盘空间
     *@return long
    **/
    private long showTableSize() throws SQLException{
        long size = 0;
        String sql = String.format(PostgresqlCons.SQL_SHOW_TABLE_SIZE,schemaName, tableName);
        try(ResultSet resultSet =  statement.executeQuery(sql)){
            if (resultSet.next()){
                size = resultSet.getLong("size");
            }
        }
        return size;
    }

    /**
     * 查询表中的主键名
     *@return String
     *
    **/
    private List<String> showTablePrimaryKey() throws SQLException{
        List<String> primaryKey = new ArrayList<>();
        ResultSet resultSet = connection.getMetaData().getPrimaryKeys(currentDatabase, schemaName, tableName);
        while(resultSet.next()){
            primaryKey.add(resultSet.getString("COLUMN_NAME"));
        }
        return primaryKey;
    }

    /**
     * 查询表中有多少条数据
     *@return long
     *
    **/
    private long showTableDataCount() throws SQLException{
        long dataCount = 0;
        String sql = String.format(PostgresqlCons.SQL_SHOW_COUNT, tableName);
        //由于主键所在系统表不具备schema隔离性，所以在查询前需要设置查询路径为当前schema
        if (!isChanged){
            setSearchPath();
        }
        try (ResultSet countSet = statement.executeQuery(sql)) {
            if (countSet.next()) {
                dataCount = countSet.getLong("count");
            }
        }
        return dataCount;
    }

    /**
     * 查询表中索引名
     *@return List<String>
     *
     **/
    private List<IndexMetaData> showIndexes() throws SQLException{
        List<IndexMetaData> result = new ArrayList<>();
        //索引名对columnName的映射
        HashMap<String,ArrayList<String>> indexColumns = new HashMap<>(16);
        //索引名对索引类型的映射
        HashMap<String,String> indexType = new HashMap<>(16);

        ResultSet resultSet = connection.getMetaData().getIndexInfo(currentDatabase, schemaName, tableName, false, false);
        while(resultSet.next()){
            ArrayList<String> columns = indexColumns.get(resultSet.getString("INDEX_NAME"));
            if(columns != null){
                columns.add(resultSet.getString("COLUMN_NAME"));
            }else{
                ArrayList<String> list = new ArrayList<>();
                list.add(resultSet.getString("COLUMN_NAME"));
                indexColumns.put(resultSet.getString("INDEX_NAME"),list);
               }
           }

        String sql = String.format(PostgresqlCons.SQL_SHOW_INDEX,schemaName,tableName);
        ResultSet indexResultSet = statement.executeQuery(sql);
        while (indexResultSet.next()){
            indexType.put(indexResultSet.getString("indexname")
                    , CommonUtils.indexType(indexResultSet.getString("indexdef")));
        }

        for(String key : indexColumns.keySet()){
              result.add(new IndexMetaData(key, indexColumns.get(key), indexType.get(key)));
          }

        return result;
    }

    /**
     * 设置查询路径为当前schema
     *
    **/
    private void setSearchPath() throws SQLException{
        String sql = String.format(PostgresqlCons.SQL_SET_SEARCHPATH, schemaName);
        statement.execute(sql);
        //置为true表示查询路径已经为当前schema
        isChanged = true;
    }

  /**
   * 由于postgresql没有类似于MySQL的"use database"的SQL语句，所以切换数据库需要重新建立连接
   *@return Connection
   *
  **/
    private  Connection getConnectionForCurrentDataBase() throws SQLException{
        ClassUtil.forName(connectionInfo.getDriver());
        //新的jdbcURL
        String url = CommonUtils.dbUrlTransform(connectionInfo.getJdbcUrl(),currentDatabase);
        connectionInfo.setJdbcUrl(url);
        return MetadataDbUtil.getConnection(connectionInfo);
    }


}

package com.dtstack.flinkx.metadatapostgresql.inputformat;

import com.dtstack.flinkx.metadata.MetaDataCons;
import com.dtstack.flinkx.metadata.inputformat.BaseMetadataInputFormat;
import com.dtstack.flinkx.metadata.inputformat.MetadataInputSplit;
import com.dtstack.flinkx.metadata.util.ConnUtil;
import com.dtstack.flinkx.metadatapostgresql.constants.PostgresqlCons;
import com.dtstack.flinkx.metadatapostgresql.pojo.ColumnMetaData;
import com.dtstack.flinkx.metadatapostgresql.pojo.TableMetaData;
import com.dtstack.flinkx.metadatapostgresql.utils.CommonUtils;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * flinkx-all com.dtstack.flinkx.metadatapostgresql.inputformat
 *
 * @author shitou
 * @description //TODO
 * @date 2020/12/9 16:25
 */
public class MetadataPostgresqlInputFormat extends BaseMetadataInputFormat {


    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        try {
            currentDb.set(((MetadataInputSplit) inputSplit).getDbName());
            //切换数据库，重新建立连接
            connection.set(getConnection(currentDb.get()));
            statement.set(connection.get().createStatement());
            tableList = ((MetadataInputSplit) inputSplit).getTableList();
            if (CollectionUtils.isEmpty(tableList)) {
                tableList = showTables();
                queryTable = true;
            }
            LOG.info("current database = {}, tableSize = {}, tableList = {}", currentDb.get(), tableList.size(), tableList);
            tableIterator.set(tableList.iterator());
            start = 0;
            init();
        } catch (ClassNotFoundException e) {
            LOG.error("could not find suitable driver, e={}", ExceptionUtil.getErrorMessage(e));
            throw new IOException(e);
        } catch (SQLException e) {
            LOG.error("获取table列表异常, dbUrl = {}, username = {}, inputSplit = {}, e = {}", dbUrl, username, inputSplit, ExceptionUtil.getErrorMessage(e));
            tableList = new LinkedList<>();
        }
        LOG.info("curentDb = {}, tableList = {}", currentDb.get(), tableList);
        tableIterator.set(tableList.iterator());
    }


    @Override
    protected Row nextRecordInternal(Row row) {
        String schema, table;
        Map<String, Object> metaData = new HashMap<>(16);
        metaData.put(MetaDataCons.KEY_OPERA_TYPE, MetaDataCons.DEFAULT_OPERA_TYPE);

        if (queryTable) {
            Pair<String, String> pair = (Pair) tableIterator.get().next();
            schema = pair.getKey();
            table = pair.getValue();
        } else {
            Map<String, String> map = (Map<String, String>) tableIterator.get().next();
            schema = map.get(PostgresqlCons.KEY_SCHEMA_NAME);
            table = map.get(PostgresqlCons.KEY_TABLE_NAME);
        }

        metaData.put(MetaDataCons.KEY_SCHEMA, schema);
        metaData.put(MetaDataCons.KEY_TABLE, table);
        try {
            metaData.putAll(showDataBaseMetaData(currentDb.get()));
            metaData.putAll(queryMetaData(table));
            metaData.put(MetaDataCons.KEY_QUERY_SUCCESS, true);
        } catch (Exception e) {
            metaData.put(MetaDataCons.KEY_QUERY_SUCCESS, false);
            metaData.put(MetaDataCons.KEY_ERROR_MSG, ExceptionUtil.getErrorMessage(e));
            LOG.error(ExceptionUtil.getErrorMessage(e));
        }
        return Row.of(metaData);
    }





    /**
     * @description 查询当前database中所有表名
     * @param :
     * @return List<Object>
     **/
    @Override
    protected List<Object> showTables() throws SQLException {
        List<Object> tableNameList = new LinkedList<>();
        try (ResultSet resultSet = statement.get().executeQuery(PostgresqlCons.SQL_SHOW_TABLES)) {

            //如果数据库中没有表，抛出异常
            if (!resultSet.next()) {
                throw new SQLException();
            }
            //指针回调
            resultSet.previous();
            while (resultSet.next()) {
                tableNameList.add(Pair.of(resultSet.getString("table_schema"), resultSet.getString("table_name")));
            }
        }

        return tableNameList;
    }


    /**
     * @description: postgresql没有对应的切换database的sql语句，所以此方法暂不实现
     * @param databaseName:
     * @return void
     **/
    @Override
    protected void switchDatabase(String databaseName) throws SQLException {

    }

    /**
     * @description 查询表中字段的元数据
     * @param tableName: 表名
     * @return Map<String , Object>
     **/
    @Override
    protected Map<String, Object> queryMetaData(String tableName) throws SQLException {

        HashMap<String, Object> result = new HashMap<>(16);
        //所有查询操作：
        String primaryKey = showTablePrimaryKey(tableName);
        int dataCount = showTableDataCount(tableName);
        String size = showTableSize(tableName);
        LinkedList<ColumnMetaData> columns = showColumnMetaData(tableName);


        TableMetaData tableMetaData = new TableMetaData(tableName, primaryKey, dataCount, size, columns);

        result.put(PostgresqlCons.KEY_METADATA, tableMetaData);


        return result;
     }


    /**
     *@description 查询表中所有字段的元数据
     *@param tableName: 表名
     *@return java.util.LinkedList<ColumnMetaData>
     *
    **/
    private LinkedList<ColumnMetaData> showColumnMetaData(String tableName)throws SQLException{
        LinkedList<ColumnMetaData> columns = new LinkedList<>();
        String sql = String.format(PostgresqlCons.SQL_SHOW_TABLE_COLUMN,tableName);
        try(ResultSet resultSet = statement.get().executeQuery(sql)){
            while(resultSet.next()){
                columns.add(new ColumnMetaData(resultSet.getString("name")
                        ,resultSet.getString("type")
                        ,resultSet.getInt("length" ) < 0 ? resultSet.getInt("lengthvar") : resultSet.getInt("length")
                        ,resultSet.getBoolean("notnull")
                        ,resultSet.getString("comment")));

            }

        }


        return columns;
    }



    /**
     *@description 查询表所占磁盘空间
     *@param tableName: 表名
     *@return java.lang.String
     *
    **/
    private String showTableSize(String tableName) throws SQLException{
        String size = "";
        String sql = String.format(PostgresqlCons.SQL_SHOW_TABLE_SIZE,tableName);
        try(ResultSet resultSet =  statement.get().executeQuery(sql)){
            if (resultSet.next()){
                size = resultSet.getString("size");
            }
        }
        return size;
    }

    /**
     *@description 查询表中的主键名
     *@param tableName: 表名
     *@return java.lang.String
     *
    **/
    private String showTablePrimaryKey(String tableName) throws SQLException{
        String primaryKey = "";
        String sql = String.format(PostgresqlCons.SQL_SHOW_TABLE_PRIMARYKEY, tableName);

        try (ResultSet keySet = statement.get().executeQuery(sql)) {
            if (keySet.next()) {
                primaryKey = keySet.getString("name");
            }
        }


        return primaryKey;
    }

    /**
     *@description 查询表中有多少条数据
     *@param tableName: 表名
     *@return int
     *
    **/
    private int showTableDataCount(String tableName) throws SQLException{
        int dataCount = 0;
        String sql = String.format(PostgresqlCons.SQL_SHOW_COUNT, tableName);

        try (ResultSet countSet = statement.get().executeQuery(sql)) {
            if (countSet.next()) {
                dataCount = countSet.getInt("count");
            }

        }

        return dataCount;
    }


  /**
   *@description 由于postgresql没有类似于MySQL的use database的SQL语句，所以切换数据库需要重新建立连接
   *@param dbName: 数据库名
   *@return java.sql.Connection
   *
  **/
    private  Connection getConnection(String dbName) throws SQLException, ClassNotFoundException{
        Class.forName(driverName);
        String url = CommonUtils.dbUrlTransform(dbUrl,dbName);
        return ConnUtil.getConnection(url,username,password);
    }


    /**
     *@description 查询当前数据库的元数据
     *@param dbName:
     *@return java.util.Map<String,String>
     *
     **/
    private Map<String,String> showDataBaseMetaData(String dbName) throws SQLException{
        Map<String,String> result = new HashMap<>(16);
        String sql = String.format(PostgresqlCons.SQL_SHOW_DATABASE_SIZE,dbName);

        try(ResultSet resultSet = statement.get().executeQuery(sql)){
            if (resultSet.next()){
                result.put(PostgresqlCons.KEY_DATABASE_NAME,resultSet.getString("name"));
                result.put(PostgresqlCons.KEY_DATABASE_OWNER,resultSet.getString("owner"));
                result.put(PostgresqlCons.KEY_DATABASE_SIZE,resultSet.getString("size"));
            }

        }

        return result;
    }

    @Override
    protected String quote(String name) {
        return null;
    }
}

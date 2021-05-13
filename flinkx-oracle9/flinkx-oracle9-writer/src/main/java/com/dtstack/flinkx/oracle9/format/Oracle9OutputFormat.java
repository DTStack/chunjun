package com.dtstack.flinkx.oracle9.format;

import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.oracle9.Oracle9DatabaseMeta;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormat;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.ReflectionUtils;
import com.dtstack.flinkx.util.RetryUtil;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.types.Row;
import sun.misc.URLClassPath;

import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/4/30 17:29
 */
public class Oracle9OutputFormat extends JdbcOutputFormat {

    @Override
    protected void openInternal(int taskNumber, int numTasks){
        try {
            dbConn = getConnection();
            //默认关闭事务自动提交，手动控制事务
            dbConn.setAutoCommit(false);

            if(CollectionUtils.isEmpty(fullColumn)) {
                fullColumn = probeFullColumns(getTable(), dbConn);
            }

            if (!EWriteMode.INSERT.name().equalsIgnoreCase(mode)){
                if(updateKey == null || updateKey.size() == 0) {
                    updateKey = probePrimaryKeys(getTable(), dbConn);
                }
            }

            if(fullColumnType == null) {
                fullColumnType = analyzeTable();
            }

            for(String col : column) {
                for (int i = 0; i < fullColumn.size(); i++) {
                    if (col.equalsIgnoreCase(fullColumn.get(i))){
                        columnType.add(fullColumnType.get(i));
                        break;
                    }
                }
            }

            preparedStatement = prepareTemplates();
            readyCheckpoint = false;

            LOG.info("subTask[{}}] wait finished", taskNumber);
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        }finally {
            DbUtil.commit(dbConn);
        }
    }


    @Override
    protected Object getField(Row row, int index) {
        Object field = super.getField(row, index);
        String type = columnType.get(index);

        //oracle timestamp to oracle varchar or varchar2 or long field format
        if (!(field instanceof Timestamp)){
            return field;
        }

        if (type.equalsIgnoreCase(ColumnType.VARCHAR.name()) || type.equalsIgnoreCase(ColumnType.VARCHAR2.name())){
            SimpleDateFormat format = DateUtil.getDateTimeFormatter();
            field= format.format(field);
        }

        if (type.equalsIgnoreCase(ColumnType.LONG.name()) ){
            field = ((Timestamp) field).getTime();
        }
        return field;
    }

    @Override
    protected List<String> probeFullColumns(String table, Connection dbConn) throws SQLException {
        String schema =null;

        String[] parts = table.split("\\.");
        if(parts.length == Oracle9DatabaseMeta.DB_TABLE_PART_SIZE) {
            schema = parts[0].toUpperCase();
            table = parts[1];
        }

        List<String> ret = new ArrayList<>();
        ResultSet rs = dbConn.getMetaData().getColumns(null, schema, table, null);
        while(rs.next()) {
            ret.add(rs.getString("COLUMN_NAME"));
        }
        return ret;
    }

    @Override
    protected Map<String, List<String>> probePrimaryKeys(String table, Connection dbConn) throws SQLException {
        Map<String, List<String>> map = new HashMap<>(16);

        try (PreparedStatement ps = dbConn.prepareStatement(String.format(GET_INDEX_SQL, table));
             ResultSet rs = ps.executeQuery()) {
            while(rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                if(!map.containsKey(indexName)) {
                    map.put(indexName,new ArrayList<>());
                }
                map.get(indexName).add(rs.getString("COLUMN_NAME"));
            }

            Map<String,List<String>> retMap = new HashMap<>((map.size()<<2)/3);
            for(Map.Entry<String,List<String>> entry: map.entrySet()) {
                String k = entry.getKey();
                List<String> v = entry.getValue();
                if(v!=null && v.size() != 0 && v.get(0) != null) {
                    retMap.put(k, v);
                }
            }
            return retMap;
        }
    }

    /**
     * 获取数据库连接
     * @return Connection
     */
    public Connection getConnection(){
        Field declaredField = ReflectionUtils.getDeclaredField(getClass().getClassLoader(), "ucp");
        assert declaredField != null;
        declaredField.setAccessible(true);
        URLClassPath urlClassPath;
        try {
            urlClassPath = (URLClassPath) declaredField.get(getClass().getClassLoader());
        } catch (IllegalAccessException e) {
            String message = String.format("can not get urlClassPath from current classLoader, classLoader = %s, e = %s", getClass().getClassLoader(), ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(message, e);
        }
        declaredField.setAccessible(false);

        List<URL> needJar = Lists.newArrayList();
        for (URL url : urlClassPath.getURLs()) {
            String urlFileName = FilenameUtils.getName(url.getPath());
            if (urlFileName.startsWith("flinkx-oracle9-writer")) {
                needJar.add(url);
                break;
            }
        }

        ClassLoader parentClassLoader = getClass().getClassLoader();
        List<String> list = new LinkedList<>();
        list.add("org.apache.flink");
        list.add("com.dtstack.flinkx");

        URLClassLoader childFirstClassLoader = FlinkUserCodeClassLoaders.childFirst(needJar.toArray(new URL[0]), parentClassLoader, list.toArray(new String[0]));

        ClassUtil.forName(driverName, childFirstClassLoader);
        try {
            return RetryUtil.executeWithRetry(() -> DriverManager.getConnection(dbUrl, username, password), 3, 2000,false);
        }catch (Exception e){
            String message = String.format("can not get oracle connection , dbUrl = %s, e = %s", dbUrl, ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(message, e);
        }
    }
}

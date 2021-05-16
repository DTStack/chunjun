package com.dtstack.flinkx.oracle9.format;

import com.dtstack.flinkx.classloader.PluginUtil;
import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.oracle9.IOracle9Helper;
import com.dtstack.flinkx.oracle9.Oracle9DatabaseMeta;
import com.dtstack.flinkx.oracle9.OracleUtil;
import com.dtstack.flinkx.rdb.outputformat.JdbcOutputFormat;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.ReflectionUtils;
import com.dtstack.flinkx.util.RetryUtil;
import com.dtstack.flinkx.util.SysUtil;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import sun.misc.URLClassPath;

import java.io.File;
import java.io.IOException;
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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/4/30 17:29
 */
public class Oracle9OutputFormat extends JdbcOutputFormat {

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
    @Override
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

                String currentPath = SysUtil.getCurrentPath();
                LOG.info("ccurrent path is {}",currentPath);

                File file = new File(currentPath + File.separator + "oracle9.zip");
                if(!file.exists()){
                    throw new RuntimeException("File oracle9.zip not exists,please sure upload this file");
                }
                //获取zip进行解压缩  加载进去
                try {
                    String savePath = file.getAbsolutePath().substring(0, file.getAbsolutePath().lastIndexOf(".")) + File.separator;
                    File file1 = new File(savePath);
                    file1.deleteOnExit();
                    if(!file1.mkdir()){
                        throw new RuntimeException("create file [ "+file1.getAbsolutePath() + "] failed");
                    }
                    SysUtil.unZip(file.getAbsolutePath(),savePath);
                    Set<URL> collect = new HashSet<>();
                    for (String s1 : PluginUtil.getAllJarNames(file1)) {
                        URL url1 = new URL("file:"+file1.getAbsolutePath()+ File.separator + s1);
                        collect.add(url1);
                    }
                    needJar.addAll(collect);
                    LOG.info("need jars {} ", GsonUtil.GSON.toJson(needJar));
                } catch (IOException e) {
                    throw new RuntimeException("");
                }

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
            IOracle9Helper helper = OracleUtil.getOracleHelperOfWrite(childFirstClassLoader);
            return helper.getConnection(dbUrl, username, password);
        }catch (Exception e){
            String message = String.format("can not get oracle connection , dbUrl = %s, e = %s", dbUrl, ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(message, e);
        }
    }
}

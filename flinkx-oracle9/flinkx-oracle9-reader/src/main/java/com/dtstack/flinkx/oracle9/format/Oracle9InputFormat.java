package com.dtstack.flinkx.oracle9.format;

import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.oracle9.IOracle9Helper;
import com.dtstack.flinkx.oracle9.OracleUtil;
import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormat;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.ReflectionUtils;
import com.dtstack.flinkx.util.RetryUtil;
import com.google.common.collect.Lists;
import org.apache.commons.io.FilenameUtils;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.types.Row;
import sun.misc.URLClassPath;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

import static com.dtstack.flinkx.rdb.util.DbUtil.clobToString;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/4/30 17:22
 */
public class Oracle9InputFormat extends JdbcInputFormat {

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        if (!hasNext) {
            return null;
        }
        row = new Row(columnCount);

        try {
            for (int pos = 0; pos < row.getArity(); pos++) {
                Object obj = resultSet.getObject(pos + 1);
                if(obj != null) {
                    if((obj instanceof java.util.Date
                            || obj.getClass().getSimpleName().toUpperCase().contains("TIMESTAMP")) ) {
                        obj = resultSet.getTimestamp(pos + 1);
                    }
                    obj = clobToString(obj);
                    //XMLType transform to String
                    obj = xmlTypeToString(obj);
                }
                row.setField(pos, obj);
            }
            return super.nextRecordInternal(row);
        }catch (Exception e) {
            throw new IOException("Couldn't read data - " + e.getMessage(), e);
        }
    }

    /**
     * 构建时间边界字符串
     * @param location          边界位置(起始/结束)
     * @param incrementColType  增量字段类型
     * @return
     */
    @Override
    protected String getTimeStr(Long location, String incrementColType){
        String timeStr;
        Timestamp ts = new Timestamp(DbUtil.getMillis(location));
        ts.setNanos(DbUtil.getNanos(location));
        timeStr = DbUtil.getNanosTimeStr(ts.toString());

        if(ColumnType.TIMESTAMP.name().equals(incrementColType)){
            //纳秒精度为9位
            timeStr = String.format("TO_TIMESTAMP('%s','YYYY-MM-DD HH24:MI:SS:FF9')", timeStr);
        } else {
            timeStr = timeStr.substring(0, 19);
            timeStr = String.format("TO_DATE('%s','YYYY-MM-DD HH24:MI:SS')", timeStr);
        }

        return timeStr;
    }

    /**
     * XMLType to String
     * @param obj xmltype
     * @return
     * @throws Exception
     */
    public Object xmlTypeToString(Object obj) throws Exception{
        String dataStr;
        if(obj instanceof oracle.xdb.XMLType){
            oracle.xdb.XMLType xml = (oracle.xdb.XMLType)obj;
            BufferedReader bf = new BufferedReader(xml.getCharacterStream());
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while ((line = bf.readLine()) != null){
                stringBuilder.append(line);
            }
            dataStr = stringBuilder.toString();
        } else {
            return obj;
        }

        return dataStr;
    }

    /**
     * 获取数据库连接，用于子类覆盖
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
            if (urlFileName.startsWith("flinkx-oracle9-reader")) {
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
            IOracle9Helper helper = OracleUtil.getOracleHelperOfReader(childFirstClassLoader);
            return RetryUtil.executeWithRetry(()->helper.getConnection(dbUrl, username, password), 3, 2000,false);
        }catch (Exception e){
            String message = String.format("can not get oracle connection , dbUrl = %s, e = %s", dbUrl, ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(message, e);
        }
    }

}

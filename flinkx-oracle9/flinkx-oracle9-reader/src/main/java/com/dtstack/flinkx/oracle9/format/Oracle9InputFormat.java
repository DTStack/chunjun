package com.dtstack.flinkx.oracle9.format;

import com.dtstack.flinkx.classloader.PluginUtil;
import com.dtstack.flinkx.enums.ColumnType;
import com.dtstack.flinkx.oracle9.IOracle9Helper;
import com.dtstack.flinkx.oracle9.OracleUtil;
import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormat;
import com.dtstack.flinkx.rdb.inputformat.JdbcInputSplit;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.ReflectionUtils;
import com.dtstack.flinkx.util.RetryUtil;
import com.dtstack.flinkx.util.SysUtil;
import com.google.common.collect.Lists;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.types.Row;
import sun.misc.URLClassPath;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static com.dtstack.flinkx.rdb.util.DbUtil.clobToString;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/4/30 17:22
 */
public class Oracle9InputFormat extends JdbcInputFormat {


    protected static final int SECOND_WAIT = 30;

    private URLClassLoader childFirstClassLoader;
    private IOracle9Helper helper;


    //压缩文件名称
    private final String ZIP_NAME = "flinkx-oracle9reader.zip";
    //taskmanager本地路径
    private String currentPath;
    //解压后的jar包路径
    private String needLoadJarPath;
    //解压jar包临时路径
    private String unzipTempPath;
    //解压完成路径
    private String actionPath;


    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        currentPath = SysUtil.getCurrentPath();
        LOG.info("ccurrent path is {}", currentPath);

    }

    @Override
    public void openInternal(InputSplit inputSplit) throws IOException {
        LOG.info("inputSplit = {}", inputSplit);
        actionBeforeReadData();

        //环境中没有oracle的jar包
//        ClassUtil.forName(driverName, getClass().getClassLoader());

        initMetric(inputSplit);
        if (!canReadData(inputSplit)) {
            LOG.warn("Not read data when the start location are equal to end location");
            hasNext = false;
            return;
        }
        querySql = buildQuerySql(inputSplit);
        try {
            executeQuery(((JdbcInputSplit) inputSplit).getStartLocation());
            if (!resultSet.isClosed()) {
                columnCount = resultSet.getMetaData().getColumnCount();
            }
        } catch (SQLException se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        }

        boolean splitWithRowCol = numPartitions > 1 && StringUtils.isNotEmpty(splitKey) && splitKey.contains("(");
        if (splitWithRowCol) {
            columnCount = columnCount - 1;
        }
        checkSize(columnCount, metaColumns);
        columnTypeList = DbUtil.analyzeColumnType(resultSet, metaColumns);
        LOG.info("JdbcInputFormat[{}]open: end", jobName);
    }

    @Override
    public Row nextRecordInternal(Row row) throws IOException {
        if (!hasNext) {
            return null;
        }
        row = new Row(columnCount);
        ClassLoader contextClassLoader = null;
        try {
            contextClassLoader  = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(childFirstClassLoader);

            for (int pos = 0; pos < row.getArity(); pos++) {
                Object obj = resultSet.getObject(pos + 1);
                if (obj != null) {
                    if ((obj instanceof java.util.Date
                            || obj.getClass().getSimpleName().toUpperCase().contains("TIMESTAMP"))) {
                        obj = resultSet.getTimestamp(pos + 1);
                    }
                    obj = clobToString(obj);
                    //XMLType transform to String
                    obj = helper.xmlTypeToString(obj);
                }
                if(pos == 0){
                    obj=new BigInteger("10000"+((BigDecimal)obj).longValue());
                }
                row.setField(pos, obj);
            }
            return super.nextRecordInternal(row);
        } catch (Exception e) {
            throw new IOException("Couldn't read data - " + e.getMessage(), e);
        }finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    /**
     * 构建时间边界字符串
     *
     * @param location         边界位置(起始/结束)
     * @param incrementColType 增量字段类型
     * @return
     */
    @Override
    protected String getTimeStr(Long location, String incrementColType) {
        String timeStr;
        Timestamp ts = new Timestamp(DbUtil.getMillis(location));
        ts.setNanos(DbUtil.getNanos(location));
        timeStr = DbUtil.getNanosTimeStr(ts.toString());

        if (ColumnType.TIMESTAMP.name().equals(incrementColType)) {
            //纳秒精度为9位
            timeStr = String.format("TO_TIMESTAMP('%s','YYYY-MM-DD HH24:MI:SS:FF9')", timeStr);
        } else {
            timeStr = timeStr.substring(0, 19);
            timeStr = String.format("TO_DATE('%s','YYYY-MM-DD HH24:MI:SS')", timeStr);
        }

        return timeStr;
    }

    /**
     * 获取数据库连接，用于子类覆盖
     *
     * @return Connection
     */
    @Override
    public Connection getConnection() {
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

                Set<URL> collect = new HashSet<>();
                for (String s1 : PluginUtil.getAllJarNames(new File(needLoadJarPath))) {
                    try {
                        collect.add(new URL("file:" + needLoadJarPath + File.separator + s1));
                    } catch (MalformedURLException e) {
                        throw new RuntimeException("get  [" + "file:" + needLoadJarPath + File.separator + s1 + "] failed", e);
                    }
                }
                needJar.addAll(collect);
                LOG.info("need jars {} ", GsonUtil.GSON.toJson(needJar));

                break;
            }
        }

        ClassLoader parentClassLoader = getClass().getClassLoader();
        List<String> list = new LinkedList<>();
        list.add("org.apache.flink");
        list.add("com.dtstack.flinkx");



        childFirstClassLoader = FlinkUserCodeClassLoaders.childFirst(needJar.toArray(new URL[0]), parentClassLoader, list.toArray(new String[0]));

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(childFirstClassLoader);

        ClassUtil.forName(driverName, childFirstClassLoader);

        try {
            helper = OracleUtil.getOracleHelper(childFirstClassLoader);
            return RetryUtil.executeWithRetry(() -> helper.getConnection(dbUrl, username, password), 3, 2000, false);
        } catch (Exception e) {
            String message = String.format("can not get oracle connection , dbUrl = %s, e = %s", dbUrl, ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(message, e);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    /**
     * 解压缩oracle9reader的驱动包
     * 如果解压缩的路径存在就不需要解压
     * 只有编号为0的task才会进行解压，先解压到oracle9reader/.unzip临时目录下 然后再移到oracle9reader目录下
     */
    protected void actionBeforeReadData() {
        File zipFile = new File(currentPath);
        zipFile = SysUtil.findFile(zipFile, ZIP_NAME);
        if (zipFile == null) {
            throw new RuntimeException("File " + zipFile.getAbsolutePath() + "  not exists,please sure upload this file");
        }

        needLoadJarPath = zipFile.getAbsolutePath().substring(0, zipFile.getAbsolutePath().lastIndexOf(".zip"));
        actionPath = needLoadJarPath + File.separator + "action";
        unzipTempPath = needLoadJarPath + File.separator + ".unzip";

        if (indexOfSubTask > 0) {
            waitForActionFinishedBeforeRead();
            return;
        }

        //获取zip进行解压缩
        try {
            File needLoadJarDirectory = new File(needLoadJarPath);

            if (!needLoadJarDirectory.exists()) {
                if (!needLoadJarDirectory.mkdir()) {
                    throw new RuntimeException("create directory [ " + needLoadJarDirectory.getAbsolutePath() + "] failed");
                }

                File unzipDirectory = new File(unzipTempPath);
                if (!unzipDirectory.mkdir()) {
                    throw new RuntimeException("create directory [ " + unzipTempPath + "] failed");
                }


                //解压到.unzip目录下
                List<String> jars = SysUtil.unZip(zipFile.getAbsolutePath(), unzipTempPath);

                //移动到oracle9reader目录下
                for (String jarPath : jars) {
                    File file = new File(jarPath);
                    file.renameTo(new File(needLoadJarPath + File.separator + file.getName()));
                }

                unzipDirectory.delete();

                File actionFile = new File(actionPath);
                if (!actionFile.mkdir()) {
                    throw new RuntimeException("create file [ " + actionFile.getAbsolutePath() + "] failed");
                }
            }
        } catch (IOException e) {
            new File(needLoadJarPath).deleteOnExit();
            throw new RuntimeException(e);
        }

    }


    protected void waitForActionFinishedBeforeRead() {

        File unzipFile = new File(needLoadJarPath);
        File actionFile = new File(actionPath);
        int n = 0;
        while (!unzipFile.exists() || !actionFile.exists()) {
            if (n > SECOND_WAIT) {
                throw new RuntimeException("Wait action finished before write timeout");
            }
            SysUtil.sleep(3000);
            n++;
        }
    }

}

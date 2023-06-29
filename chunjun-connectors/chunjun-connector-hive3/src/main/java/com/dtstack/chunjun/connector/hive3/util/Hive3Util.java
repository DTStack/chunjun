/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.hive3.util;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.hive3.config.HdfsConfig;
import com.dtstack.chunjun.connector.hive3.converter.HdfsOrcSqlConverter;
import com.dtstack.chunjun.connector.hive3.converter.HdfsOrcSyncConverter;
import com.dtstack.chunjun.connector.hive3.converter.HdfsParquetSqlConverter;
import com.dtstack.chunjun.connector.hive3.converter.HdfsParquetSyncConverter;
import com.dtstack.chunjun.connector.hive3.converter.HdfsTextSqlConverter;
import com.dtstack.chunjun.connector.hive3.converter.HdfsTextSyncConverter;
import com.dtstack.chunjun.connector.hive3.enums.FileType;
import com.dtstack.chunjun.connector.hive3.source.HdfsPathFilter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.enums.ColumnType;
import com.dtstack.chunjun.security.KerberosUtil;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;
import com.dtstack.chunjun.util.ColumnTypeUtil;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.table.types.logical.RowType;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.io.api.Binary;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.security.PrivilegedAction;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.dtstack.chunjun.security.KerberosUtil.KRB_STR;

@Slf4j
public class Hive3Util {
    public static final String NULL_VALUE = "\\N";

    private static final long NANO_SECONDS_PER_DAY = 86400_000_000_000L;

    private static final long JULIAN_EPOCH_OFFSET_DAYS = 2440588;

    private static final double SCALE_TWO = 2.0;
    private static final double SCALE_TEN = 10.0;
    private static final int BIT_SIZE = 8;

    private static final String KEY_HADOOP_SECURITY_AUTHORIZATION = "hadoop.security.authorization";
    private static final String KEY_HADOOP_SECURITY_AUTHENTICATION =
            "hadoop.security.authentication";
    private static final String KEY_DEFAULT_FS = "fs.default.name";
    private static final String KEY_FS_HDFS_IMPL_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";
    private static final String KEY_HA_DEFAULT_FS = "fs.defaultFS";
    private static final String KEY_DFS_NAME_SERVICES = "dfs.nameservices";
    private static final String KEY_HADOOP_USER_NAME = "hadoop.user.name";

    public static Object getWritableValue(Object writable) {
        Class<?> clz = writable.getClass();
        Object ret;

        if (clz == IntWritable.class) {
            ret = ((IntWritable) writable).get();
        } else if (clz == Text.class) {
            ret = writable.toString();
        } else if (clz == LongWritable.class) {
            ret = ((LongWritable) writable).get();
        } else if (clz == ByteWritable.class) {
            ret = ((ByteWritable) writable).get();
        } else if (clz == DateWritable.class) {
            ret = ((DateWritable) writable).get();
        } else if (writable instanceof DoubleWritable) {
            ret = ((DoubleWritable) writable).get();
        } else if (writable instanceof TimestampWritable) {
            ret = ((TimestampWritable) writable).getTimestamp();
        } else if (writable instanceof TimestampWritableV2) {
            ret = ((TimestampWritableV2) writable).getTimestamp();
        } else if (writable instanceof HiveDecimalWritable) {
            ret = ((HiveDecimalWritable) writable).getHiveDecimal().bigDecimalValue();
        } else if (writable instanceof DateWritable) {
            ret = ((DateWritable) writable).get();
        } else if (writable instanceof DateWritableV2) {
            ret = ((DateWritableV2) writable).get();
        } else if (writable instanceof FloatWritable) {
            ret = ((FloatWritable) writable).get();
        } else if (writable instanceof BooleanWritable) {
            ret = ((BooleanWritable) writable).get();
        } else if (writable instanceof BytesWritable) {
            BytesWritable bytesWritable = (BytesWritable) writable;
            byte[] bytes = bytesWritable.getBytes();
            // org.apache.hadoop.io.BytesWritable.setSize方法中扩容导致byte[]末尾自动补0，这里需要把末尾的0去掉才能得到真正的byte[]
            ret = new byte[bytesWritable.getLength()];
            System.arraycopy(bytes, 0, ret, 0, bytesWritable.getLength());
        } else if (writable instanceof HiveDecimalWritable) {
            ret = ((HiveDecimalWritable) writable).getHiveDecimal().bigDecimalValue();
        } else if (writable instanceof ShortWritable) {
            ret = ((ShortWritable) writable).get();
        } else {
            ret = writable.toString();
        }
        return ret;
    }

    public static AbstractRowConverter createRowConverter(
            boolean useAbstractBaseColumn,
            String fileType,
            List<FieldConfig> fieldConfList,
            RawTypeMapper rawTypeMapper,
            HdfsConfig hdfsConfig) {
        AbstractRowConverter rowConverter;
        RowType rowType = TableUtil.createRowType(fieldConfList, rawTypeMapper);
        if (useAbstractBaseColumn) {
            switch (FileType.getByName(fileType)) {
                case ORC:
                    rowConverter = new HdfsOrcSyncConverter(rowType, hdfsConfig);
                    break;
                case PARQUET:
                    rowConverter = new HdfsParquetSyncConverter(rowType, hdfsConfig);
                    break;
                case TEXT:
                    rowConverter = new HdfsTextSyncConverter(rowType, hdfsConfig);
                    break;
                default:
                    throw new UnsupportedTypeException(fileType);
            }
        } else {
            switch (FileType.getByName(fileType)) {
                case ORC:
                    rowConverter = new HdfsOrcSqlConverter(rowType, hdfsConfig);
                    break;
                case PARQUET:
                    rowConverter = new HdfsParquetSqlConverter(rowType, hdfsConfig);
                    break;
                case TEXT:
                    rowConverter = new HdfsTextSqlConverter(rowType, hdfsConfig);
                    break;
                default:
                    throw new UnsupportedTypeException(fileType);
            }
        }
        return rowConverter;
    }

    /**
     * Encapsulate common exceptions in hdfs operation and give solutions
     *
     * @param customizeMessage
     * @param errorMsg
     * @return
     */
    public static String parseErrorMsg(String customizeMessage, String errorMsg) {
        StringBuilder str = new StringBuilder();
        str.append(customizeMessage);
        Pair<String, String> pair = null;
        if (StringUtils.isNotBlank(customizeMessage)) {
            str.append(customizeMessage);
        }
        if (StringUtils.isNotBlank(errorMsg)) {
            if (errorMsg.contains(
                    "at org.apache.hadoop.hdfs.server.namenode.FSNamesystem.checkLease")) {
                pair =
                        Pair.of(
                                "The file or directory may not exist or may be inaccessible ",
                                "make sure there is no other task operating same hdfs dir at same time");
            }
        }
        if (pair != null) {
            str.append("\nthe Cause maybe : ")
                    .append(pair.getLeft())
                    .append(", \nand the Solution maybe : ")
                    .append(pair.getRight())
                    .append(", ");
        }

        return str.toString();
    }

    public static Binary timeToBinary(Timestamp timestamp) {
        long nano = timestamp.getTime() * 1000_000;

        int julianDays = (int) ((nano / NANO_SECONDS_PER_DAY) + JULIAN_EPOCH_OFFSET_DAYS);
        byte[] julianDaysBytes = getBytes(julianDays);
        flip(julianDaysBytes);

        long lastDayNanos = nano % NANO_SECONDS_PER_DAY;
        byte[] lastDayNanosBytes = getBytes(lastDayNanos);
        flip(lastDayNanosBytes);

        byte[] dst = new byte[12];

        System.arraycopy(lastDayNanosBytes, 0, dst, 0, 8);
        System.arraycopy(julianDaysBytes, 0, dst, 8, 4);

        return Binary.fromConstantByteArray(dst);
    }

    private static byte[] getBytes(long i) {
        byte[] bytes = new byte[8];
        bytes[0] = (byte) ((i >> 56) & 0xFF);
        bytes[1] = (byte) ((i >> 48) & 0xFF);
        bytes[2] = (byte) ((i >> 40) & 0xFF);
        bytes[3] = (byte) ((i >> 32) & 0xFF);
        bytes[4] = (byte) ((i >> 24) & 0xFF);
        bytes[5] = (byte) ((i >> 16) & 0xFF);
        bytes[6] = (byte) ((i >> 8) & 0xFF);
        bytes[7] = (byte) (i & 0xFF);
        return bytes;
    }

    /** @param bytes */
    private static void flip(byte[] bytes) {
        for (int i = 0, j = bytes.length - 1; i < j; i++, j--) {
            byte t = bytes[i];
            bytes[i] = bytes[j];
            bytes[j] = t;
        }
    }

    public static Binary decimalToBinary(final HiveDecimal hiveDecimal, int prec, int scale) {
        byte[] decimalBytes = hiveDecimal.setScale(scale).unscaledValue().toByteArray();

        // Estimated number of bytes needed.
        int precToBytes = ParquetHiveSerDe.PRECISION_TO_BYTE_COUNT[prec - 1];
        if (precToBytes == decimalBytes.length) {
            // No padding needed.
            return Binary.fromReusedByteArray(decimalBytes);
        }

        byte[] tgt = new byte[precToBytes];
        if (hiveDecimal.signum() == -1) {
            // For negative number, initializing bits to 1
            for (int i = 0; i < precToBytes; i++) {
                tgt[i] |= 0xFF;
            }
        }

        // Padding leading zeroes/ones.
        System.arraycopy(
                decimalBytes, 0, tgt, precToBytes - decimalBytes.length, decimalBytes.length);
        return Binary.fromReusedByteArray(tgt);
    }

    public static int computeMinBytesForPrecision(int precision) {
        int numBytes = 1;
        while (Math.pow(SCALE_TWO, BIT_SIZE * numBytes - 1.0) < Math.pow(SCALE_TEN, precision)) {
            numBytes += 1;
        }
        return numBytes;
    }

    public static ObjectInspector columnTypeToObjectInspector(
            ColumnType columnType, ColumnTypeUtil.DecimalInfo decimalInfo) {
        ObjectInspector objectInspector;
        switch (columnType) {
            case TINYINT:
                objectInspector =
                        ObjectInspectorFactory.getReflectionObjectInspector(
                                Byte.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case SMALLINT:
                objectInspector =
                        ObjectInspectorFactory.getReflectionObjectInspector(
                                Short.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case INT:
                objectInspector =
                        ObjectInspectorFactory.getReflectionObjectInspector(
                                Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case BIGINT:
                objectInspector =
                        ObjectInspectorFactory.getReflectionObjectInspector(
                                Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case FLOAT:
                objectInspector =
                        ObjectInspectorFactory.getReflectionObjectInspector(
                                Float.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case DOUBLE:
                objectInspector =
                        ObjectInspectorFactory.getReflectionObjectInspector(
                                Double.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case DECIMAL:
                if (decimalInfo != null) {
                    try {
                        Constructor<WritableHiveDecimalObjectInspector> constructor =
                                WritableHiveDecimalObjectInspector.class.getDeclaredConstructor(
                                        DecimalTypeInfo.class);
                        constructor.setAccessible(true);
                        objectInspector =
                                constructor.newInstance(
                                        new DecimalTypeInfo(
                                                decimalInfo.getPrecision(),
                                                decimalInfo.getScale()));
                    } catch (Exception e) {
                        log.warn(
                                "can't create WritableHiveDecimalObjectInspector from {}",
                                decimalInfo,
                                e);
                        objectInspector =
                                ObjectInspectorFactory.getReflectionObjectInspector(
                                        HiveDecimalWritable.class,
                                        ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                    }
                } else {
                    objectInspector =
                            ObjectInspectorFactory.getReflectionObjectInspector(
                                    HiveDecimalWritable.class,
                                    ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                }
                break;
            case TIMESTAMP:
                objectInspector =
                        ObjectInspectorFactory.getReflectionObjectInspector(
                                org.apache.hadoop.hive.common.type.Timestamp.class,
                                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case DATE:
                objectInspector =
                        ObjectInspectorFactory.getReflectionObjectInspector(
                                Date.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                objectInspector =
                        ObjectInspectorFactory.getReflectionObjectInspector(
                                String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case BOOLEAN:
                objectInspector =
                        ObjectInspectorFactory.getReflectionObjectInspector(
                                Boolean.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case BINARY:
                objectInspector =
                        ObjectInspectorFactory.getReflectionObjectInspector(
                                BytesWritable.class,
                                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            default:
                throw new IllegalArgumentException("You should not be here");
        }
        return objectInspector;
    }

    public static Set<String> getAllPartitionPath(
            String inputPath, FileSystem fileSystem, HdfsPathFilter pathFilter) {
        // 因为拿到的是所有文件，分区可能重复。此处用 hashSet 避免分区路径重复。
        HashSet<String> paths = new HashSet<>();
        try {
            findAllPartitionPath(paths, fileSystem, inputPath, pathFilter);
        } catch (IOException e) {
            log.error(
                    "retrieve all partition error, hdfs input path {},errMsg {}",
                    inputPath,
                    ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException("retrieve ClassLoad happens error");
        }
        return paths;
    }

    /**
     * 递归 hdfs 分区目录
     *
     * @param paths 所有的分区目录
     * @param fileSystem 文件系统
     * @param path 传入的 path
     * @throws IOException IOException
     */
    private static void findAllPartitionPath(
            HashSet<String> paths, FileSystem fileSystem, String path, HdfsPathFilter pathFilter)
            throws IOException {
        FileStatus[] allChildPath = fileSystem.listStatus(new Path(path), pathFilter);
        if (allChildPath != null && allChildPath.length > 0) {
            for (FileStatus filePath : allChildPath) {
                if (filePath.isDirectory()
                        && getFileName(filePath.getPath().toString()).contains("=")) {
                    findAllPartitionPath(
                            paths, fileSystem, filePath.getPath().toString(), pathFilter);
                } else {
                    paths.add(filePath.getPath().getParent().toString());
                    return;
                }
            }
        }
    }

    public static JobConf getJobConf(Map<String, Object> confMap, String defaultFs) {
        confMap = fillConfig(confMap, defaultFs);

        JobConf jobConf = new JobConf();
        confMap.forEach(
                (key, val) -> {
                    if (val != null) {
                        jobConf.set(key, val.toString());
                    }
                });

        return jobConf;
    }

    private static String getFileName(String path) {
        return path.substring(path.lastIndexOf("/"));
    }

    public static Configuration getConfiguration(Map<String, Object> confMap, String defaultFs) {
        confMap = fillConfig(confMap, defaultFs);

        Configuration conf = new Configuration();
        confMap.forEach(
                (key, val) -> {
                    if (val != null) {
                        conf.set(key, val.toString());
                    }
                });

        return conf;
    }

    public static boolean isOpenKerberos(Map<String, Object> hadoopConfig) {
        if (!MapUtils.getBoolean(hadoopConfig, KEY_HADOOP_SECURITY_AUTHORIZATION, false)) {
            return false;
        }

        return KRB_STR.equalsIgnoreCase(
                MapUtils.getString(hadoopConfig, KEY_HADOOP_SECURITY_AUTHENTICATION));
    }

    public static void setHadoopUserName(Configuration conf) {
        String hadoopUserName = conf.get(KEY_HADOOP_USER_NAME);
        if (StringUtils.isEmpty(hadoopUserName)) {
            return;
        }

        try {
            String previousUserName = UserGroupInformation.getLoginUser().getUserName();
            log.info(
                    "Hadoop user from '{}' switch to '{}' with SIMPLE auth",
                    previousUserName,
                    hadoopUserName);
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hadoopUserName);
            UserGroupInformation.setLoginUser(ugi);
        } catch (Exception e) {
            log.warn("Set hadoop user name error:", e);
        }
    }

    private static FileSystem getFsWithUser(
            Map<String, Object> hadoopConfig, String defaultFs, String user) throws Exception {
        if (StringUtils.isEmpty(user)) {
            return FileSystem.get(getConfiguration(hadoopConfig, defaultFs));
        }
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
        return ugi.doAs(
                (PrivilegedAction<FileSystem>)
                        () -> {
                            try {
                                return FileSystem.get(getConfiguration(hadoopConfig, defaultFs));
                            } catch (Exception e) {
                                throw new RuntimeException("Get FileSystem  error:", e);
                            }
                        });
    }

    public static FileSystem getFileSystem(
            Map<String, Object> hadoopConfigMap,
            String defaultFs,
            String user,
            DistributedCache distributedCache,
            String jobId,
            String taskNumber)
            throws Exception {
        if (isOpenKerberos(hadoopConfigMap)) {
            return getFsWithKerberos(
                    hadoopConfigMap, defaultFs, distributedCache, jobId, taskNumber);
        }
        return getFsWithUser(hadoopConfigMap, defaultFs, user);
    }

    public static FileSystem getFileSystem(
            Map<String, Object> hadoopConfigMap,
            String defaultFs,
            DistributedCache distributedCache,
            String jobId,
            String taskNumber)
            throws Exception {
        if (isOpenKerberos(hadoopConfigMap)) {
            return getFsWithKerberos(
                    hadoopConfigMap, defaultFs, distributedCache, jobId, taskNumber);
        }

        Configuration conf = getConfiguration(hadoopConfigMap, defaultFs);
        setHadoopUserName(conf);

        return FileSystem.get(getConfiguration(hadoopConfigMap, defaultFs));
    }

    private static FileSystem getFsWithKerberos(
            Map<String, Object> hadoopConfig,
            String defaultFs,
            DistributedCache distributedCache,
            String jobId,
            String taskNumber)
            throws Exception {
        UserGroupInformation ugi =
                getUGI(hadoopConfig, defaultFs, distributedCache, jobId, taskNumber);

        return ugi.doAs(
                (PrivilegedAction<FileSystem>)
                        () -> {
                            try {
                                return FileSystem.get(getConfiguration(hadoopConfig, defaultFs));
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "Get FileSystem with kerberos error:", e);
                            }
                        });
    }

    public static UserGroupInformation getUGI(
            Map<String, Object> hadoopConfig,
            String defaultFs,
            DistributedCache distributedCache,
            String jobId,
            String taskNumber)
            throws IOException {
        String keytabFileName = KerberosUtil.getPrincipalFileName(hadoopConfig);
        keytabFileName =
                KerberosUtil.loadFile(
                        hadoopConfig, keytabFileName, distributedCache, jobId, taskNumber);
        String principal = KerberosUtil.getPrincipal(hadoopConfig, keytabFileName);
        KerberosUtil.loadKrb5Conf(hadoopConfig, distributedCache, jobId, taskNumber);
        KerberosUtil.refreshConfig();

        return KerberosUtil.loginAndReturnUgi(
                getConfiguration(hadoopConfig, defaultFs), principal, keytabFileName);
    }

    private static Map<String, Object> fillConfig(Map<String, Object> confMap, String defaultFs) {
        if (confMap == null) {
            confMap = new HashMap<>();
        }

        if (isHaMode(confMap)) {
            if (defaultFs != null) {
                confMap.put(KEY_HA_DEFAULT_FS, defaultFs);
            }
        } else {
            if (defaultFs != null) {
                confMap.put(KEY_DEFAULT_FS, defaultFs);
            }
        }

        confMap.put(KEY_FS_HDFS_IMPL_DISABLE_CACHE, "true");
        return confMap;
    }

    private static boolean isHaMode(Map<String, Object> confMap) {
        return StringUtils.isNotEmpty(MapUtils.getString(confMap, KEY_DFS_NAME_SERVICES));
    }
}

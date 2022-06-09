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

package com.dtstack.chunjun.connector.inceptor.util;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.inceptor.converter.InceptorOrcColumnConvent;
import com.dtstack.chunjun.connector.inceptor.converter.InceptorOrcRowConverter;
import com.dtstack.chunjun.connector.inceptor.converter.InceptorParquetColumnConverter;
import com.dtstack.chunjun.connector.inceptor.converter.InceptorParquetRowConverter;
import com.dtstack.chunjun.connector.inceptor.converter.InceptorTextColumnConvent;
import com.dtstack.chunjun.connector.inceptor.converter.InceptorTextRowConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.enums.ColumnType;
import com.dtstack.chunjun.security.KerberosUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarchar2Writable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.io.api.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.PrivilegedAction;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.dtstack.chunjun.security.KerberosUtil.KRB_STR;

public class InceptorUtil {
    public static final Logger LOG = LoggerFactory.getLogger(InceptorUtil.class);
    public static final String NULL_VALUE = "\\N";

    private static final double SCALE_TWO = 2.0;
    private static final double SCALE_TEN = 10.0;
    private static final int BIT_SIZE = 8;

    public static final int JULIAN_EPOCH_OFFSET_DAYS = 2440588;
    public static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1L);
    public static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1L);
    public static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1L);
    private static final String KEY_DEFAULT_FS = "fs.default.name";
    private static final String KEY_FS_HDFS_IMPL_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";
    private static final String KEY_HA_DEFAULT_FS = "fs.defaultFS";
    private static final String KEY_DFS_NAME_SERVICES = "dfs.nameservices";
    private static final String KEY_HADOOP_USER_NAME = "hadoop.user.name";
    private static final String KEY_HADOOP_SECURITY_AUTHORIZATION = "hadoop.security.authorization";
    private static final String KEY_HADOOP_SECURITY_AUTHENTICATION =
            "hadoop.security.authentication";

    /**
     * createRowConverter
     *
     * @param useAbstractBaseColumn
     * @param fileType
     * @param fieldConfList
     * @param converter
     * @return
     */
    public static AbstractRowConverter createRowConverter(
            boolean useAbstractBaseColumn,
            String fileType,
            List<FieldConf> fieldConfList,
            RawTypeConverter converter) {
        AbstractRowConverter rowConverter;
        if (useAbstractBaseColumn) {
            if ("ORC".equals(fileType.toUpperCase(Locale.ENGLISH))) {
                rowConverter = new InceptorOrcColumnConvent(fieldConfList);
            } else if ("PARQUET".equals(fileType.toUpperCase(Locale.ENGLISH))) {
                rowConverter = new InceptorParquetColumnConverter(fieldConfList);
            } else {
                rowConverter = new InceptorTextColumnConvent(fieldConfList);
            }
        } else {
            RowType rowType = TableUtil.createRowType(fieldConfList, converter);
            switch (fileType.toUpperCase(Locale.ENGLISH)) {
                case "ORC":
                    rowConverter = new InceptorOrcRowConverter(rowType);
                    break;
                case "PARQUET":
                    rowConverter = new InceptorParquetRowConverter(rowType);
                    break;
                default:
                    rowConverter = new InceptorTextRowConverter(rowType);
                    break;
            }
        }
        return rowConverter;
    }

    public static Object getWritableValue(Object writable) {
        if (writable == null) {
            return null;
        }
        Class<?> clz = writable.getClass();
        Object ret = null;
        if (clz == IntWritable.class) {
            ret = ((IntWritable) writable).get();
        } else if (clz == Text.class) {
            ret = ((Text) writable).toString();
        } else if (clz == LongWritable.class) {
            ret = ((LongWritable) writable).get();
        } else if (clz == ByteWritable.class) {
            ret = ((ByteWritable) writable).get();
        } else if (clz == DateWritable.class) {
            // tdh 日期类型为hivedate，要转化成Date
            ret = new java.util.Date(((DateWritable) writable).get().getTime());
        } else if (writable instanceof DoubleWritable) {
            ret = ((DoubleWritable) writable).get();
        } else if (writable instanceof TimestampWritable) {
            ret = ((TimestampWritable) writable).getTimestamp();
        } else if (writable instanceof DateWritable) {
            ret = new java.util.Date(((DateWritable) writable).get().getTime());
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
        } else if (writable instanceof ByteWritable) {
            ByteWritable byteWritable = (ByteWritable) ret;
            ret = String.valueOf(byteWritable.get());
        } else {
            ret = writable.toString();
        }
        return ret;
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
        if (org.apache.commons.lang3.StringUtils.isNotBlank(customizeMessage)) {
            str.append(customizeMessage);
        }
        if (org.apache.commons.lang3.StringUtils.isNotBlank(errorMsg)) {
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

    public static int computeMinBytesForPrecision(int precision) {
        int numBytes = 1;
        while (Math.pow(SCALE_TWO, BIT_SIZE * numBytes - 1.0) < Math.pow(SCALE_TEN, precision)) {
            numBytes += 1;
        }
        return numBytes;
    }

    public static ObjectInspector columnTypeToObjectInspetor(ColumnType columnType) {
        ObjectInspector objectInspector = null;
        switch (columnType) {
            case TINYINT:
                objectInspector =
                        ObjectInspectorFactory.getReflectionObjectInspector(
                                ByteWritable.class,
                                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case SMALLINT:
                objectInspector =
                        ObjectInspectorFactory.getReflectionObjectInspector(
                                ShortWritable.class,
                                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
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
                                DoubleWritable.class,
                                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case DECIMAL:
                objectInspector =
                        ObjectInspectorFactory.getReflectionObjectInspector(
                                HiveDecimalWritable.class,
                                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case TIMESTAMP:
                objectInspector =
                        ObjectInspectorFactory.getReflectionObjectInspector(
                                Timestamp.class,
                                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case DATE:
                objectInspector =
                        ObjectInspectorFactory.getReflectionObjectInspector(
                                DateWritable.class,
                                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case STRING:
            case CHAR:
                objectInspector =
                        ObjectInspectorFactory.getReflectionObjectInspector(
                                String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case VARCHAR2:
                objectInspector =
                        ObjectInspectorFactory.getReflectionObjectInspector(
                                HiveVarchar2Writable.class,
                                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case VARCHAR:
                objectInspector =
                        ObjectInspectorFactory.getReflectionObjectInspector(
                                HiveVarcharWritable.class,
                                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
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

    public static Binary timestampToInt96(TimestampData timestampData) {
        int julianDay;
        long nanosOfDay;

        // Use UTC timezone or local timezone to the conversion between epoch time and
        // LocalDateTime.
        // Hive 0.x/1.x/2.x use local timezone. But Hive 3.x use UTC timezone.
        Timestamp timestamp = timestampData.toTimestamp();
        long mills = timestamp.getTime();
        julianDay = (int) ((mills / MILLIS_IN_DAY) + JULIAN_EPOCH_OFFSET_DAYS);
        nanosOfDay = ((mills % MILLIS_IN_DAY) / 1000) * NANOS_PER_SECOND + timestamp.getNanos();

        ByteBuffer buf = ByteBuffer.allocate(12);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(nanosOfDay);
        buf.putInt(julianDay);
        buf.flip();
        return Binary.fromConstantByteBuffer(buf);
    }

    public static FileSystem getFileSystem(
            Map<String, Object> hadoopConfigMap,
            String defaultFs,
            DistributedCache distributedCache)
            throws Exception {
        if (isOpenKerberos(hadoopConfigMap)) {
            return getFsWithKerberos(hadoopConfigMap, defaultFs, distributedCache);
        }

        Configuration conf = getConfiguration(hadoopConfigMap, defaultFs);
        setHadoopUserName(conf);

        return FileSystem.get(getConfiguration(hadoopConfigMap, defaultFs));
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

    private static FileSystem getFsWithKerberos(
            Map<String, Object> hadoopConfig, String defaultFs, DistributedCache distributedCache)
            throws Exception {
        UserGroupInformation ugi = getUGI(hadoopConfig, defaultFs, distributedCache);

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
            Map<String, Object> hadoopConfig, String defaultFs, DistributedCache distributedCache)
            throws IOException {
        String keytabFileName = KerberosUtil.getPrincipalFileName(hadoopConfig);
        keytabFileName = KerberosUtil.loadFile(hadoopConfig, keytabFileName, distributedCache);
        String principal = KerberosUtil.getPrincipal(hadoopConfig, keytabFileName);
        KerberosUtil.loadKrb5Conf(hadoopConfig, distributedCache);
        KerberosUtil.refreshConfig();

        return KerberosUtil.loginAndReturnUgi(
                getConfiguration(hadoopConfig, defaultFs), principal, keytabFileName);
    }

    public static boolean isOpenKerberos(Map<String, Object> hadoopConfig) {
        if (!MapUtils.getBoolean(hadoopConfig, KEY_HADOOP_SECURITY_AUTHORIZATION, false)) {
            return false;
        }

        return KRB_STR.equalsIgnoreCase(
                MapUtils.getString(hadoopConfig, KEY_HADOOP_SECURITY_AUTHENTICATION));
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

    public static void setHadoopUserName(Configuration conf) {
        String hadoopUserName = conf.get(KEY_HADOOP_USER_NAME);
        if (StringUtils.isEmpty(hadoopUserName)) {
            return;
        }

        try {
            String previousUserName = UserGroupInformation.getLoginUser().getUserName();
            LOG.info(
                    "Hadoop user from '{}' switch to '{}' with SIMPLE auth",
                    previousUserName,
                    hadoopUserName);
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hadoopUserName);
        } catch (Exception e) {
            LOG.warn("Set hadoop user name error:", e);
        }
    }

    private static boolean isHaMode(Map<String, Object> confMap) {
        return StringUtils.isNotEmpty(MapUtils.getString(confMap, KEY_DFS_NAME_SERVICES));
    }
}

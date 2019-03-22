/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.hdfs;

import com.dtstack.flinkx.common.ColumnType;
import com.dtstack.flinkx.util.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.ql.io.HiveBinaryOutputFormat;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.*;

import java.io.File;
import java.io.FilenameFilter;
import java.sql.Date;
import java.text.SimpleDateFormat;

/**
 * Utilities for HdfsReader and HdfsWriter
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class HdfsUtil {

    private static final String HADOOP_CONFIGE = System.getProperty("user.dir") + "/conf/hadoop/";

    private static final String HADOOP_CONF_DIR = System.getenv("HADOOP_CONF_DIR");

    private static Configuration configuration = new Configuration();

    static {
        try {
            String dir = StringUtils.isNotBlank(HADOOP_CONF_DIR)?HADOOP_CONF_DIR:HADOOP_CONFIGE;
            configuration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
            configuration.set("fs.hdfs.impl.disable.cache", "true");
            File[] xmlFileList = new File(dir).listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    if(name.endsWith(".xml")){
                        return true;
                    }
                    return false;
                }
            });

            if(xmlFileList != null) {
                for(File xmlFile : xmlFileList) {
                    configuration.addResource(xmlFile.toURI().toURL());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public static Configuration getConfiguration(){
        return configuration;
    }

    public static String getDefaultFs(){
        return configuration.get("fs.defaultFS");
    }

    public static Object string2col(String str, String type, SimpleDateFormat customDateFormat) {
        if (str == null || str.length() == 0){
            return null;
        }

        if(type == null){
            return str;
        }

        ColumnType columnType = ColumnType.fromString(type.toUpperCase());
        Object ret;
        switch(columnType) {
            case TINYINT:
                ret = Byte.valueOf(str.trim());
                break;
            case SMALLINT:
                ret = Short.valueOf(str.trim());
                break;
            case INT:
                ret = Integer.valueOf(str.trim());
                break;
            case BIGINT:
                ret = Long.valueOf(str.trim());
                break;
            case FLOAT:
                ret = Float.valueOf(str.trim());
                break;
            case DOUBLE:
            case DECIMAL:
                ret = Double.valueOf(str.trim());
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                if(customDateFormat != null){
                    ret = DateUtil.columnToDate(str,customDateFormat);
                    ret = DateUtil.timestampToString((Date)ret);
                } else {
                    ret = str;
                }
                break;
            case BOOLEAN:
                ret = Boolean.valueOf(str.trim().toLowerCase());
                break;
            case DATE:
                ret = DateUtil.columnToDate(str,customDateFormat);
                break;
            case TIMESTAMP:
                ret = DateUtil.columnToTimestamp(str,customDateFormat);
                break;
            default:
                throw new IllegalArgumentException("Unsupported field type:" + type);
        }

        return ret;
    }

    public static Object getWritableValue(Object writable) {
        Class<?> clz = writable.getClass();
        Object ret = null;

        if(clz == IntWritable.class) {
            ret = ((IntWritable) writable).get();
        } else if(clz == Text.class) {
            ret = ((Text) writable).toString();
        } else if(clz == LongWritable.class) {
            ret = ((LongWritable) writable).get();
        } else if(clz == ByteWritable.class) {
            ret = ((ByteWritable) writable).get();
        } else if(clz == DateWritable.class) {
            ret = ((DateWritable) writable).get();
        } else if(writable instanceof Writable) {
            ret = writable.toString();
        } else {
            ret = writable.toString();
        }

        return ret;
    }

    public static ObjectInspector columnTypeToObjectInspetor(ColumnType columnType) {
        ObjectInspector objectInspector = null;
        switch(columnType) {
            case TINYINT:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Byte.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case SMALLINT:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Short.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case INT:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case BIGINT:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case FLOAT:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Float.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case DOUBLE:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Double.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case DECIMAL:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(HiveDecimalWritable.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case TIMESTAMP:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(java.sql.Timestamp.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case DATE:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(java.sql.Date.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case BOOLEAN:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Boolean.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case BINARY:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(BytesWritable.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            default:
                throw new IllegalArgumentException("You should not be here");
        }
        return objectInspector;
    }

}

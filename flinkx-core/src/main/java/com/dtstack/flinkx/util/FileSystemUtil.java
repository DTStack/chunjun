/*
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


package com.dtstack.flinkx.util;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import java.util.Map;

/**
 * @author jiangbo
 * @date 2019/11/19
 */
public class FileSystemUtil {

    private static final String KEY_FS_HDFS_IMPL_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";
    private static final String KEY_DEFAULT_FS = "fs.default.name";
    private static final String KEY_HA_DEFAULT_FS = "fs.defaultFS";
    private static final String KEY_DFS_NAMESERVICES = "dfs.nameservices";

    public static Configuration getConfiguration(Map<String, String> confMap, String defaultFs) {
        fillConfig(confMap, defaultFs);

        Configuration conf = new Configuration();
        if(confMap == null){
            return conf;
        }

        confMap.forEach((key, val) -> {
            if(val != null){
                conf.set(key, val);
            }
        });

        return conf;
    }

    public static JobConf getJobConf(Map<String, String> confMap, String defaultFs){
        fillConfig(confMap, defaultFs);

        JobConf jobConf = new JobConf();
        if (confMap == null) {
            return jobConf;
        }

        confMap.forEach((key, val) -> {
            if(val != null){
                jobConf.set(key, val);
            }
        });

        return jobConf;
    }

    private static void fillConfig(Map<String, String> confMap, String defaultFs) {
        if (confMap == null) {
            return;
        }

        if (isHaMode(confMap)) {
            confMap.put(KEY_HA_DEFAULT_FS, defaultFs);
        } else {
            confMap.put(KEY_DEFAULT_FS, defaultFs);
        }

        confMap.put(KEY_FS_HDFS_IMPL_DISABLE_CACHE, "true");
    }

    private static boolean isHaMode(Map<String, String> confMap){
        return StringUtils.isNotEmpty(MapUtils.getString(confMap, KEY_DFS_NAMESERVICES));
    }
}

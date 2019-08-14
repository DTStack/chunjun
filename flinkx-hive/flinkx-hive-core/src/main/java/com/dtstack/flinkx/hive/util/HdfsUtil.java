/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.hive.util;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
 * @author toutian
 */
public class HdfsUtil {

    public static Configuration getHadoopConfig(Map<String, String> confMap, String defaultFS) {
        Configuration conf = new Configuration();

        if (confMap != null) {
            for (Map.Entry<String, String> entry : confMap.entrySet()) {
                conf.set(entry.getKey(), entry.getValue());
            }
        }

        conf.set("fs.default.name", defaultFS);
        conf.set("fs.hdfs.impl.disable.cache", "true");

        return conf;
    }

}

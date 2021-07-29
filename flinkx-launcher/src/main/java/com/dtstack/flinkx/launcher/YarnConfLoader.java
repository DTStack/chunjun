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

package com.dtstack.flinkx.launcher;

import com.dtstack.flinkx.constants.ConstantValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.util.Iterator;
import java.util.Map;

/**
 * load yarn conf from specify dir
 * Date: 2018/11/17
 * Company: www.dtstack.com
 * @author xuchao
 */

public class YarnConfLoader {

    public static YarnConfiguration getYarnConf(String yarnConfDir) {
        YarnConfiguration yarnConf = new YarnConfiguration();
        try {

            File dir = new File(yarnConfDir);
            if(dir.exists() && dir.isDirectory()) {

                File[] xmlFileList = new File(yarnConfDir).listFiles((dir1, name) -> {
                    if(name.endsWith(ConstantValue.FILE_SUFFIX_XML)){
                        return true;
                    }
                    return false;
                });

                if(xmlFileList != null) {
                    for(File xmlFile : xmlFileList) {
                        yarnConf.addResource(xmlFile.toURI().toURL());
                    }
                }
            }

        } catch(Exception e) {
            throw new RuntimeException(e);
        }

        haYarnConf(yarnConf);
        return yarnConf;
    }

    /**
     * deal yarn HA conf
     */
    private static Configuration haYarnConf(Configuration yarnConf) {
        Iterator<Map.Entry<String, String>> iterator = yarnConf.iterator();
        while(iterator.hasNext()) {
            Map.Entry<String,String> entry = iterator.next();
            String key = entry.getKey();
            String value = entry.getValue();
            if(key.startsWith("yarn.resourcemanager.hostname.")) {
                String rm = key.substring("yarn.resourcemanager.hostname.".length());
                String addressKey = "yarn.resourcemanager.address." + rm;
                if(yarnConf.get(addressKey) == null) {
                    yarnConf.set(addressKey, value + ":" + YarnConfiguration.DEFAULT_RM_PORT);
                }
            }
        }
        return yarnConf;
    }
}

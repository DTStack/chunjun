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

package com.dtstack.chunjun.yarn;

import com.dtstack.chunjun.constants.ConstantValue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.util.Map;

/** load yarn conf from specify dir */
public class YarnConfLoader {

    public static YarnConfiguration loadConf(String yarnConfDir) {
        YarnConfiguration yarnConf = new YarnConfiguration();
        try {

            File dir = new File(yarnConfDir);
            if (dir.exists() && dir.isDirectory()) {

                File[] xmlFileList =
                        new File(yarnConfDir)
                                .listFiles(
                                        (dir1, name) ->
                                                name.endsWith(ConstantValue.FILE_SUFFIX_XML));

                if (xmlFileList != null) {
                    for (File xmlFile : xmlFileList) {
                        yarnConf.addResource(xmlFile.toURI().toURL());
                    }
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        setHaYarnConf(yarnConf);
        return yarnConf;
    }

    /** deal yarn HA conf */
    private static void setHaYarnConf(Configuration yarnConf) {
        for (Map.Entry<String, String> entry : yarnConf) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith("yarn.resourcemanager.hostname.")) {
                String rm = key.substring("yarn.resourcemanager.hostname.".length());
                String addressKey = "yarn.resourcemanager.address." + rm;
                if (yarnConf.get(addressKey) == null) {
                    yarnConf.set(addressKey, value + ":" + YarnConfiguration.DEFAULT_RM_PORT);
                }
            }
        }
    }
}

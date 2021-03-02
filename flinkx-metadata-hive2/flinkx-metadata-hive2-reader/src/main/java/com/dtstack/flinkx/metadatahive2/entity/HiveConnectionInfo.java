/*
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

package com.dtstack.flinkx.metadatahive2.entity;


import com.dtstack.metadata.rdb.core.entity.ConnectionInfo;

import java.util.Map;

/**
 * @company:www.dtstack.com
 * @Author:shiFang
 * @Date:2021-01-20 14:57
 * @Description:
 */
public class HiveConnectionInfo extends ConnectionInfo {

    public HiveConnectionInfo(ConnectionInfo connectionInfo) {
        super(connectionInfo);
    }

    private Map<String, Object> hiveConf;

    public Map<String, Object> getHiveConf() {
        return hiveConf;
    }

    public void setHiveConf(Map<String, Object> hiveConf) {
        this.hiveConf = hiveConf;
    }

    @Override
    public String toString() {
        return "HiveConnectionInfo{" +
                "hiveConf=" + hiveConf +
                '}';
    }
}

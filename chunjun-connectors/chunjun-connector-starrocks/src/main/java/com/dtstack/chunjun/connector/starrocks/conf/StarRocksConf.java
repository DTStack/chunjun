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
package com.dtstack.chunjun.connector.starrocks.conf;

import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lihongwei
 * @date 2022/04/11
 */
public class StarRocksConf extends JdbcConf {

    /** fe_ip:http_port 多个地址分号连接 */
    private String loadUrl;
    /** 主键模型表需要传入主键列表 */
    private List<String> primaryKey = new ArrayList<>();

    public String getLoadUrl() {
        return loadUrl;
    }

    public void setLoadUrl(String loadUrl) {
        this.loadUrl = loadUrl;
    }

    public List<String> getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(List<String> primaryKey) {
        this.primaryKey = primaryKey;
    }

    @Override
    public String toString() {
        return "StarRocksConf{" + "loadUrl='" + loadUrl + '\'' + ", primaryKey=" + primaryKey + '}';
    }
}

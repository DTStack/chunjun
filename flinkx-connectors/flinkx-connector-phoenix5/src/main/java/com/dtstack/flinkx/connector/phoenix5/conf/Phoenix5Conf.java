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
package com.dtstack.flinkx.connector.phoenix5.conf;

import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;

/**
 * @author wujuan
 * @version 1.0
 * @date 2021/7/9 16:01 星期五
 * @email wujuan@dtstack.com
 * @company www.dtstack.com
 */
public class Phoenix5Conf extends JdbcConf {
    // 是否直接读取HBase的数据
    private boolean readFromHbase;
    private Integer scanCacheSize;
    private Integer scanBatchSize;
    private Boolean syncTaskType;

    public boolean isReadFromHbase() {
        return readFromHbase;
    }

    public void setReadFromHbase(boolean readFromHbase) {
        this.readFromHbase = readFromHbase;
    }

    public Integer getScanCacheSize() {
        return scanCacheSize;
    }

    public void setScanCacheSize(Integer scanCacheSize) {
        this.scanCacheSize = scanCacheSize;
    }

    public Integer getScanBatchSize() {
        return scanBatchSize;
    }

    public void setScanBatchSize(Integer scanBatchSize) {
        this.scanBatchSize = scanBatchSize;
    }

    public Boolean getSyncTaskType() {
        return syncTaskType;
    }

    public void setSyncTaskType(Boolean syncTaskType) {
        this.syncTaskType = syncTaskType;
    }
}

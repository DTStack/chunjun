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

package com.dtstack.chunjun.connector.oceanbasecdc.conf;

import com.dtstack.chunjun.config.CommonConfig;

import com.oceanbase.clogproxy.client.config.ObReaderConfig;

public class OceanBaseCdcConf extends CommonConfig {
    private String logProxyHost;
    private int logProxyPort;
    private ObReaderConfig obReaderConfig;
    private String cat;
    private boolean pavingData = true;
    private boolean splitUpdate;

    private String timestampFormat = "sql";

    public String getLogProxyHost() {
        return logProxyHost;
    }

    public void setLogProxyHost(String logProxyHost) {
        this.logProxyHost = logProxyHost;
    }

    public int getLogProxyPort() {
        return logProxyPort;
    }

    public void setLogProxyPort(int logProxyPort) {
        this.logProxyPort = logProxyPort;
    }

    public ObReaderConfig getObReaderConfig() {
        return obReaderConfig;
    }

    public void setObReaderConfig(ObReaderConfig obReaderConfig) {
        this.obReaderConfig = obReaderConfig;
    }

    public String getCat() {
        return cat;
    }

    public void setCat(String cat) {
        this.cat = cat;
    }

    public boolean isPavingData() {
        return pavingData;
    }

    public void setPavingData(boolean pavingData) {
        this.pavingData = pavingData;
    }

    public boolean isSplitUpdate() {
        return splitUpdate;
    }

    public void setSplitUpdate(boolean splitUpdate) {
        this.splitUpdate = splitUpdate;
    }

    public String getTimestampFormat() {
        return timestampFormat;
    }

    public void setTimestampFormat(String timestampFormat) {
        this.timestampFormat = timestampFormat;
    }
}

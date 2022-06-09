/*
 *
 *  *
 *  *  * Licensed to the Apache Software Foundation (ASF) under one
 *  *  * or more contributor license agreements.  See the NOTICE file
 *  *  * distributed with this work for additional information
 *  *  * regarding copyright ownership.  The ASF licenses this file
 *  *  * to you under the Apache License, Version 2.0 (the
 *  *  * "License"); you may not use this file except in compliance
 *  *  * with the License.  You may obtain a copy of the License at
 *  *  *
 *  *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 */

package com.dtstack.chunjun.connector.influxdb.conf;

import java.util.Locale;

/**
 * Company：www.dtstack.com.
 *
 * @author shitou
 * @date 2022/3/15
 */
public class InfluxdbSourceConfig extends InfluxdbConfig {
    private static final long serialVersionUID = 1L;

    private String where;
    private String customSql;
    private String splitPk;
    private int queryTimeOut = 3;
    private int fetchSize = 1000;
    private String epoch = "N";
    private String format = "MSGPACK";

    public String getFormat() {
        return format.toUpperCase(Locale.ENGLISH);
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getWhere() {
        return where;
    }

    public void setWhere(String where) {
        this.where = where;
    }

    public String getCustomSql() {
        return customSql;
    }

    public void setCustomSql(String customSql) {
        this.customSql = customSql;
    }

    public String getSplitPk() {
        return splitPk;
    }

    public void setSplitPk(String splitPk) {
        this.splitPk = splitPk;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public int getQueryTimeOut() {
        return queryTimeOut;
    }

    public void setQueryTimeOut(int queryTimeOut) {
        this.queryTimeOut = queryTimeOut;
    }

    public String getEpoch() {
        return epoch.toLowerCase(Locale.ENGLISH);
    }

    public void setEpoch(String epoch) {
        this.epoch = epoch;
    }

    @Override
    public String toString() {
        return "InfluxdbSourceConfig{"
                + "where='"
                + where
                + '\''
                + ", customSql='"
                + customSql
                + '\''
                + ", splitPk='"
                + splitPk
                + '\''
                + ", queryTimeOut="
                + queryTimeOut
                + ", fetchSize="
                + fetchSize
                + ", epoch='"
                + epoch
                + '\''
                + ", format='"
                + format
                + '\''
                + '}';
    }
}

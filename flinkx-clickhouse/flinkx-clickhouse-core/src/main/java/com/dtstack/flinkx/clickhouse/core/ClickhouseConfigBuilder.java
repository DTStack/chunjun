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
package com.dtstack.flinkx.clickhouse.core;

import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Properties;

/**
 * Date: 2019/11/05
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public final class ClickhouseConfigBuilder {
    //common
    private String url;
    private String username;
    private String password;
    private String table;
    private List<MetaColumn> column;
    private Properties clickhouseProp;

    //reader
    private Integer queryTimeOut;
    private String splitKey;
    private String filter;
    private String increColumn;
    private String startLocation;

    //writer
    private Integer batchInterval;
    private String preSql;
    private String postSql;

    public ClickhouseConfig build(){
        ClickhouseConfig clickhouseConfig = new ClickhouseConfig();
        clickhouseConfig.setUrl(url);
        clickhouseConfig.setUsername(username);
        clickhouseConfig.setPassword(password);
        clickhouseConfig.setTable(table);
        clickhouseConfig.setColumn(column);
        clickhouseConfig.setClickhouseProp(clickhouseProp);
        clickhouseConfig.setQueryTimeOut(queryTimeOut);
        clickhouseConfig.setSplitKey(splitKey);
        clickhouseConfig.setFilter(filter);
        clickhouseConfig.setIncreColumn(increColumn);
        clickhouseConfig.setStartLocation(startLocation);
        clickhouseConfig.setBatchInterval(batchInterval);
        clickhouseConfig.setPreSql(preSql);
        clickhouseConfig.setPostSql(postSql);
        return clickhouseConfig;
    }

    public ClickhouseConfigBuilder withUrl(String url){
        Preconditions.checkArgument(StringUtils.isNotBlank(url), "Parameter [url] can not be null or empty");
        this.url = url;
        return this;
    }
    public ClickhouseConfigBuilder withUsername(String username){
        this.username = username;
        return this;
    }
    public ClickhouseConfigBuilder withPassword(String password){
        this.password = password;
        return this;
    }
    public ClickhouseConfigBuilder withTable(String table){
        Preconditions.checkArgument(StringUtils.isNotBlank(table), "Parameter [table] can not be null or empty");
        this.table = table;
        return this;
    }
    public ClickhouseConfigBuilder withColumn(List<MetaColumn> column){
        this.column = column;
        return this;
    }
    public ClickhouseConfigBuilder withClickhouseProp(Properties clickhouseProp){
        this.clickhouseProp = clickhouseProp;
        return this;
    }
    public ClickhouseConfigBuilder withQueryTimeOut(Integer queryTimeOut){
        this.queryTimeOut = queryTimeOut;
        return this;
    }
    public ClickhouseConfigBuilder withSplitKey(String splitKey){
        this.splitKey = splitKey;
        return this;
    }
    public ClickhouseConfigBuilder withFilter(String filter){
        this.filter = filter;
        return this;
    }
    public ClickhouseConfigBuilder withIncreColumn(String increColumn){
        this.increColumn = increColumn;
        return this;
    }
    public ClickhouseConfigBuilder withStartLocation(String startLocation){
        this.startLocation = startLocation;
        return this;
    }
    public ClickhouseConfigBuilder withBatchInterval(Integer batchInterval){
        this.batchInterval = batchInterval;
        return this;
    }
    public ClickhouseConfigBuilder withPreSql(String preSql){
        this.preSql = preSql;
        return this;
    }
    public ClickhouseConfigBuilder withPostSql(String postSql){
        this.postSql = postSql;
        return this;
    }

}

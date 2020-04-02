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

package com.dtstack.flinkx.redis.writer;

import com.dtstack.flinkx.outputformat.BaseRichOutputFormatBuilder;
import com.dtstack.flinkx.redis.DataMode;
import com.dtstack.flinkx.redis.DataType;

import java.util.List;

/**
 * The builder for RedisOutputFormat
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class RedisOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private RedisOutputFormat format;

    public RedisOutputFormatBuilder() {
        super.format = format = new RedisOutputFormat();
    }

    public void setHostPort(String hostPort) {
        this.format.hostPort = hostPort;
    }

    public void setPassword(String password) {
        this.format.password = password;
    }

    public void setDatabase(int database) {
        this.format.database = database;
    }

    public void setKeyIndexes(List<Integer> keyIndexes) {
        this.format.keyIndexes = keyIndexes;
    }

    public void setKeyFieldDelimiter(String keyFieldDelimiter) {
        this.format.keyFieldDelimiter = keyFieldDelimiter;
    }

    public void setDateFormat(String dateFormat) {
        this.format.dateFormat = dateFormat;
    }

    public void setExpireTime(long expireTime) {
        this.format.expireTime = expireTime;
    }

    public void setTimeout(int timeout) {
        this.format.timeout = timeout;
    }

    public void setType(DataType type) {
        this.format.type = type;
    }

    public void setDataMode(DataMode dataMode) {
        this.format.dataMode = dataMode;
    }

    public void setValueFieldDelimiter(String valueFieldDelimiter) {
        this.format.valueFieldDelimiter = valueFieldDelimiter;
    }

    @Override
    protected void checkFormat() {
        if(format.hostPort == null){
            throw new IllegalArgumentException("No host and port supplied");
        }

        if (format.keyIndexes == null || format.keyIndexes.size() == 0){
            throw new IllegalArgumentException("Field keyIndexes cannot be empty");
        }

        if (format.getRestoreConfig() != null && format.getRestoreConfig().isRestore()){
            throw new UnsupportedOperationException("This plugin not support restore from failed state");
        }
    }
}

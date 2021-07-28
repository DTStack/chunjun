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

package com.dtstack.flinkx.connector.redis.sink;

import com.dtstack.flinkx.connector.redis.conf.RedisConf;
import com.dtstack.flinkx.sink.format.BaseRichOutputFormatBuilder;

/**
 * @author chuixue
 * @create 2021-06-16 15:14
 * @description
 */
public class RedisOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    private RedisOutputFormat format;

    public RedisOutputFormatBuilder() {
        super.format = format = new RedisOutputFormat();
    }

    public void setRedisConf(RedisConf redisConf) {
        super.setConfig(redisConf);
        format.setRedisConf(redisConf);
    }

    @Override
    protected void checkFormat() {
        RedisConf redisConf = format.getRedisConf();
        if (redisConf.getHostPort() == null) {
            throw new IllegalArgumentException("No host and port supplied");
        }

        if (redisConf.getKeyIndexes() == null || redisConf.getKeyIndexes().size() == 0) {
            throw new IllegalArgumentException("Field keyIndexes cannot be empty");
        }
    }
}

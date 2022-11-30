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

package com.dtstack.chunjun.connector.redis.source;

import com.dtstack.chunjun.connector.redis.config.RedisConfig;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;

public class RedisInputFormatBuilder extends BaseRichInputFormatBuilder<RedisInputFormat> {
    public RedisInputFormatBuilder() {
        super(new RedisInputFormat());
    }

    public void setRedisConf(RedisConfig redisConfig) {
        super.setConfig(redisConfig);
        format.setRedisConf(redisConfig);
    }

    @Override
    protected void checkFormat() {
        RedisConfig redisConfig = format.getRedisConf();
        StringBuilder sb = new StringBuilder(1024);
        if (redisConfig.getHostPort() == null) {
            sb.append("No host and port supplied\n");
        }
        if (!redisConfig.getType().toString().equals(("HASH"))) {
            sb.append("Currently only supported hash\n");
        }

        if (redisConfig.getKeyPrefix() == null) {
            sb.append("No key prefix supplied\n");
        }
        if (sb.length() > 0) {
            throw new IllegalArgumentException("\n" + sb);
        }
    }
}

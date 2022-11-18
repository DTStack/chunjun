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

package com.dtstack.chunjun.connector.redis.sink;

import com.dtstack.chunjun.connector.redis.config.RedisConfig;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

public class RedisOutputFormatBuilder extends BaseRichOutputFormatBuilder<RedisOutputFormat> {

    public RedisOutputFormatBuilder() {
        super(new RedisOutputFormat());
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
            sb.append("No host and port supplied").append("\n");
        }
        if (redisConfig.getType() == null) {
            sb.append("No type supplied").append("\n");
        }

        if (redisConfig.getMode() == null) {
            sb.append("No mode supplied").append("\n");
        }

        if (redisConfig.getKeyIndexes() == null || redisConfig.getKeyIndexes().size() == 0) {
            sb.append("Field keyIndexes cannot be empty").append("\n");
        } else {
            if (CollectionUtils.isNotEmpty(redisConfig.getColumn())) {
                List<Integer> collect =
                        redisConfig.getKeyIndexes().stream()
                                .filter(i -> i < 0 || i >= redisConfig.getColumn().size())
                                .collect(Collectors.toList());
                if (CollectionUtils.isNotEmpty(collect)) {
                    sb.append(
                                    "keyIndexes range cannot be less than 0 or greater than the size of the column, error index is [")
                            .append(StringUtils.join(collect, ","))
                            .append("]")
                            .append("\n");
                }
            }

            if (sb.length() > 0) {
                throw new IllegalArgumentException("\n" + sb);
            }
        }
    }
}

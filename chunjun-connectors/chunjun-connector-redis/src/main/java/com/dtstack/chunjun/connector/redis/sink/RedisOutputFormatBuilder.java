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

import com.dtstack.chunjun.connector.redis.conf.RedisConf;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author chuixue
 * @create 2021-06-16 15:14
 * @description
 */
public class RedisOutputFormatBuilder extends BaseRichOutputFormatBuilder<RedisOutputFormat> {

    public RedisOutputFormatBuilder() {
        super(new RedisOutputFormat());
    }

    public void setRedisConf(RedisConf redisConf) {
        super.setConfig(redisConf);
        format.setRedisConf(redisConf);
    }

    @Override
    protected void checkFormat() {
        RedisConf redisConf = format.getRedisConf();
        StringBuilder sb = new StringBuilder(1024);
        if (redisConf.getHostPort() == null) {
            sb.append("No host and port supplied").append("\n");
        }
        if (redisConf.getType() == null) {
            sb.append("No type supplied").append("\n");
        }

        if (redisConf.getMode() == null) {
            sb.append("No mode supplied").append("\n");
        }

        if (redisConf.getKeyIndexes() == null || redisConf.getKeyIndexes().size() == 0) {
            sb.append("Field keyIndexes cannot be empty").append("\n");
        } else {
            if (CollectionUtils.isNotEmpty(redisConf.getColumn())) {
                List<Integer> collect =
                        redisConf.getKeyIndexes().stream()
                                .filter(i -> i < 0 || i >= redisConf.getColumn().size())
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
                throw new IllegalArgumentException("\n" + sb.toString());
            }
        }
    }
}

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

package com.dtstack.chunjun.connector.redis.enums;

public enum RedisConnectType {
    /** 单机 */
    STANDALONE(1),
    /** 哨兵 */
    SENTINEL(2),
    /** 集群 */
    CLUSTER(3);
    final int type;

    RedisConnectType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public static RedisConnectType parse(int redisType) {
        for (RedisConnectType type : RedisConnectType.values()) {
            if (type.getType() == redisType) {
                return type;
            }
        }
        throw new RuntimeException("unsupported redis type[" + redisType + "]");
    }
}

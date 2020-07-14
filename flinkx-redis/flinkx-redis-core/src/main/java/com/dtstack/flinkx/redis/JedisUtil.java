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

package com.dtstack.flinkx.redis;

import com.dtstack.flinkx.util.TelnetUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.dtstack.flinkx.redis.RedisConfigKeys.KEY_DB;
import static com.dtstack.flinkx.redis.RedisConfigKeys.KEY_HOST_PORT;
import static com.dtstack.flinkx.redis.RedisConfigKeys.KEY_PASSWORD;
import static com.dtstack.flinkx.redis.RedisConfigKeys.KEY_TIMEOUT;

/**
 * Utilities for redis database connection
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class JedisUtil {

    private static final String DEFAULT_HOST = "localhost";

    private static final String DEFAULT_PORT = "6379";

    private static final String DEFAULT_DB = "0";

    public static final int TIMEOUT = 3000;

    public static final String DELIMITER = "\\u0001";

    private static JedisPool jedisPool;

    private static final Pattern PATTERN = Pattern.compile("(?<host>.+):(?<port>\\d+)");

    public static Jedis getJedis(Properties properties) {
        if (jedisPool == null){
            String hostPortStr = properties.getProperty(KEY_HOST_PORT,DEFAULT_HOST);
            String port = null;
            String host = null;
            Matcher matcher = PATTERN.matcher(hostPortStr);
            if(matcher.find()){
                host = matcher.group("host");
                port = matcher.group("port");
            }
            port = port == null ? DEFAULT_PORT : port;

            TelnetUtil.telnet(host,Integer.parseInt(port));

            int timeOut = (Integer) properties.getOrDefault(KEY_TIMEOUT,TIMEOUT);
            String password = properties.getProperty(KEY_PASSWORD);
            int db = Integer.parseInt(properties.getOrDefault(KEY_DB,DEFAULT_DB).toString()) ;

            jedisPool = new JedisPool(getConfig(), host, Integer.valueOf(port),timeOut, password, db);
        }
        return jedisPool.getResource();
    }

    public static void close(Jedis jedis){
        try {
            if(jedisPool != null){
                jedisPool.close();
            }

            if(jedis != null){
                jedis.close();
            }
        } catch (Exception ignore){
        }
    }

    private static JedisPoolConfig getConfig(){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(100);
        jedisPoolConfig.setMaxTotal(500);
        jedisPoolConfig.setMinIdle(0);
        jedisPoolConfig.setMaxWaitMillis(2000);
        jedisPoolConfig.setTestOnBorrow(true);
        return jedisPoolConfig;
    }
}

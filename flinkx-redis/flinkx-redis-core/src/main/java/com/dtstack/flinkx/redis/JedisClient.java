package com.dtstack.flinkx.redis;

import redis.clients.jedis.JedisCommands;

/**
 * @author jiangbo
 * @date 2018/6/29 15:28
 */
public class JedisClient {

    private JedisCommands jedis;

    public JedisClient(JedisCommands jedis) {
        this.jedis = jedis;
    }

    public void set(String key,String values){
        jedis.set(key,values);
    }
}

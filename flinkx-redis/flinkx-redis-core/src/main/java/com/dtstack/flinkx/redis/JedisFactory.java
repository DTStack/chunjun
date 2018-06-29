package com.dtstack.flinkx.redis;

import redis.clients.jedis.*;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.dtstack.flinkx.redis.RedisConfigKeys.*;

/**
 * @author jiangbo
 * @date 2018/6/29 15:27
 */
public class JedisFactory {

    private static final String DEFAULT_HOST = "localhost";

    private static final String DEFAULT_PORT = "6379";

    private static final String DEFAULT_DB = "0";

    private static final int TIMEOUT = 2000;

    private static JedisPool jedisPool;

    private static JedisCluster jedisCluster;

    private Properties properties;

    private static final Pattern PATTERN = Pattern.compile("(?<host>.+):(?<port>\\d+)");

    private static final String HOST_PORT_SPLIT = ",";

    public JedisFactory(Properties properties) {
        this.properties = properties;
    }

    public JedisCommands getJedis(ClusterModel model){
        JedisCommands jedisCommands;
        switch (model){
            case STANDALONE:
                jedisCommands = getJedis();
                break;
            case SHARDED:
                throw new RuntimeException("Shared mode is not supported at this time");
            case CLUSTER:
                jedisCommands = getJedisCluster();
                break;
            default:
                throw new RuntimeException("Unsupported cluster mode");
        }
        return jedisCommands;
    }

    private JedisCluster getJedisCluster(){
        if(jedisCluster == null){
            jedisCluster = new JedisCluster(buildHostProts(),TIMEOUT,getConfig());
        }
        return jedisCluster;
    }

    private Set<HostAndPort> buildHostProts(){
        Set<HostAndPort> hostAndPorts = new HashSet<>();
        String hostPortListStr = properties.getProperty(KEY_HOST_PORT_LIST,"");
        for (String hostPortStr : hostPortListStr.split(HOST_PORT_SPLIT)) {
            Matcher matcher = PATTERN.matcher(hostPortStr);
            String host = matcher.group("host");
            String port = matcher.group("port");

            port = port == null ? DEFAULT_PORT : port;

            hostAndPorts.add(new HostAndPort(host,Integer.parseInt(port)));
        }

        return hostAndPorts;
    }

    private Jedis getJedis(){
        if (jedisPool == null){
            String host = properties.getProperty(KEY_HOST,DEFAULT_HOST);
            int port = Integer.parseInt(properties.getProperty(KEY_PORT,DEFAULT_PORT));
            String password = properties.getProperty(KEY_PASSWORD);
            int db = Integer.parseInt(properties.getProperty(KEY_DB,DEFAULT_DB)) ;

            jedisPool = new JedisPool(getConfig(), host, port,TIMEOUT, password, db);
        }
        return jedisPool.getResource();
    }

    public void close(JedisCommands jedis){
        try {
            if(jedisPool != null){
                jedisPool.close();
            }

            if (jedis instanceof Jedis){
                ((Jedis) jedis).close();
            } else if(jedis instanceof JedisCluster){
                ((JedisCluster) jedis).close();
            }
        } catch (Exception e){

        }
    }

    private JedisPoolConfig getConfig(){
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(100);
        jedisPoolConfig.setMaxTotal(500);
        jedisPoolConfig.setMinIdle(0);
        jedisPoolConfig.setMaxWaitMillis(2000);
        jedisPoolConfig.setTestOnBorrow(true);
        return jedisPoolConfig;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}

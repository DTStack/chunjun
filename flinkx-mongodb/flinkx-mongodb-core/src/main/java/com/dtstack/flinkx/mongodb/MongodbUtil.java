package com.dtstack.flinkx.mongodb;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.dtstack.flinkx.mongodb.MongodbConfigKeys.*;

/**
 * @author jiangbo
 * @date 2018/6/5 10:40
 */
public class MongodbUtil {

    private static final Logger LOG = LoggerFactory.getLogger(MongodbUtil.class);

    private static final String HOST_SPLIT_REGEX = ",\\s*";

    private static Pattern HOST_PORT_PATTERN = Pattern.compile("(?<host>(\\d{0,3}\\.){3}\\d{0,3})(:(?<port>\\d+))*");

    private static final Integer DEFAULT_PORT = 27017;

    /**
     * Get mongo client
     * @param config
     * @return MongoClient
     */
    public static MongoClient getMongoClient(Map<String,String> config){
        try{
            MongoClient mongoClient;

            MongoClientOptions options = getOption();
            List<ServerAddress> serverAddress = getServerAddress(config.get(KEY_HOST_PORTS));
            String username = config.get(KEY_USERNAME);
            String password = config.get(KEY_PASSWORD);
            String database = config.get(KEY_DATABASE);

            if(username == null){
                mongoClient = new MongoClient(serverAddress,options);
            } else {
                MongoCredential credential = MongoCredential.createScramSha1Credential(username, database, password.toCharArray());
                List<MongoCredential> credentials = Lists.newArrayList();
                credentials.add(credential);

                mongoClient = new MongoClient(serverAddress,credentials,options);
            }

            LOG.info("mongo客户端获取成功");
            return mongoClient;
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public static MongoDatabase getDatabase(Map<String,String> config,String database){
        MongoClient client = getMongoClient(config);
        return client.getDatabase(database);
    }

    public static MongoCollection<Document> getCollection(Map<String,String> config,String database, String collection){
        MongoClient client = getMongoClient(config);
        MongoDatabase db = client.getDatabase(database);
        return db.getCollection(collection);
    }

    /**
     * parse server address from hostPorts string
     */
    private static List<ServerAddress> getServerAddress(String hostPorts){
        List<ServerAddress> addresses = Lists.newArrayList();

        for (String hostPort : hostPorts.split(HOST_SPLIT_REGEX)) {
            if(hostPort.length() == 0){
                continue;
            }

            Matcher matcher = HOST_PORT_PATTERN.matcher(hostPort);
            if(matcher.find()){
                String host = matcher.group("host");
                String portStr = matcher.group("port");
                int port = portStr == null ? DEFAULT_PORT : Integer.parseInt(portStr);

                ServerAddress serverAddress = new ServerAddress(host,port);
                addresses.add(serverAddress);
            }
        }

        return addresses;
    }

    private static MongoClientOptions getOption(){
        MongoClientOptions.Builder build = new MongoClientOptions.Builder();
        build.connectionsPerHost(1);
        build.threadsAllowedToBlockForConnectionMultiplier(1);
        build.connectTimeout(1000);
        build.maxWaitTime(1000);

        return build.build();
    }
}

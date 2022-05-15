package com.dtstack.chunjun.connector.redis.enums;

public enum RedisConnectType {
    /** 单机 */
    STANDALONE(1),
    /** 哨兵 */
    SENTINEL(2),
    /** 集群 */
    CLUSTER(3);
    int type;

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

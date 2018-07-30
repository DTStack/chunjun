package com.dtstack.flinkx.redis;

/**
 * @author jiangbo
 * @date 2018/7/3 15:32
 */
public enum  DataMode {

    /** reader mode */

    /** write mode */
    SET("set"),L_PUSH("lpush"),R_PUSH("rpush"),S_ADD("sadd"),Z_ADD("zadd"),H_SET("hset");

    private String mode;

    DataMode(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }

    public static DataMode getDataMode(String mode){
        for (DataMode dataMode : DataMode.values()) {
            if(dataMode.getMode().equals(mode)){
                return dataMode;
            }
        }

        throw new RuntimeException("Unsupported redis data mode:" + mode);
    }
}

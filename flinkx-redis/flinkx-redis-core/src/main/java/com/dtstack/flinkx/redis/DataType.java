package com.dtstack.flinkx.redis;

/**
 * @author jiangbo
 * @date 2018/7/3 15:39
 */
public enum DataType {

    STRING("string"),LIST("list"),SET("set"),Z_SET("zset"),HASH("hash");

    private String type;

    DataType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static DataType getDataType(String type){
        for (DataType dataType : DataType.values()) {
            if(dataType.getType().equals(type)){
                return dataType;
            }
        }

        throw new RuntimeException("Unsupported redis data type:" + type);
    }
}

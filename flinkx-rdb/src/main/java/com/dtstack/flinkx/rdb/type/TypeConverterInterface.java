package com.dtstack.flinkx.rdb.type;

/**
 * @author jiangbo
 * @date 2018/6/4 17:53
 */
public interface TypeConverterInterface {

    Object convert(Object data,String typeName);

}

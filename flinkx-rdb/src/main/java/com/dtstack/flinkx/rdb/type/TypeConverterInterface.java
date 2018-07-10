package com.dtstack.flinkx.rdb.type;

import java.io.Serializable;

/**
 * @author jiangbo
 * @date 2018/6/4 17:53
 */
public interface TypeConverterInterface extends Serializable {

    Object convert(Object data,String typeName);

}

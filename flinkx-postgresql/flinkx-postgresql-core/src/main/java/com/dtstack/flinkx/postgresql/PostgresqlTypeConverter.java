package com.dtstack.flinkx.postgresql;

import com.dtstack.flinkx.rdb.type.TypeConverterInterface;

import java.util.Arrays;
import java.util.List;

/**
 * @author jiangbo
 * @date 2018/6/4 18:00
 */
public class PostgresqlTypeConverter implements TypeConverterInterface {

    private List<String> stringTypes = Arrays.asList("uuid","xml","cidr","inet","macaddr");

    private List<String> byteTypes = Arrays.asList("bytea","bit varying");

    private List<String> bitTypes = Arrays.asList("bit");

    private List<String> doubleTypes = Arrays.asList("money");

    @Override
    public Object convert(Object data,String typeName) {

        if(doubleTypes.contains(typeName)){
            data = Double.parseDouble(String.valueOf(data));
        } else if(bitTypes.contains(typeName)){
            data = ((Boolean) data ? 1 : 0);
        } else if(stringTypes.contains(typeName)){
            data = String.valueOf(data);
        } else if(byteTypes.contains(typeName)){
            // TODO support byte type later
        }

        return data;
    }
}

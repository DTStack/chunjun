package com.dtstack.flinkx.kingbase;

import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The type converter for KingBase database
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class KingBaseTypeConverter implements TypeConverterInterface {

    private List<String> stringTypes = Arrays.asList("uuid","xml","cidr","inet","macaddr");

    private List<String> byteTypes = Arrays.asList("bytea","bit varying");

    private List<String> bitTypes = Collections.singletonList("bit");

    private List<String> doubleTypes = Collections.singletonList("money");

    private List<String> intTypes = Arrays.asList("int","int2","int4","int8");

    @Override
    public Object convert(Object data,String typeName) {
        if (data == null){
            return null;
        }
        String dataValue = data.toString();
        if(stringTypes.contains(typeName)){
            return dataValue;
        }
        if(StringUtils.isBlank(dataValue)){
            return null;
        }
        if(doubleTypes.contains(typeName)){
            if(StringUtils.startsWith(dataValue, "$")){
                dataValue = StringUtils.substring(dataValue, 1);
            }
            data = Double.parseDouble(dataValue);
        } else if(bitTypes.contains(typeName)){
            //
        }else if(byteTypes.contains(typeName)){
            data = Byte.valueOf(dataValue);
        } else if(intTypes.contains(typeName)){
            data = Integer.parseInt(dataValue);
        }

        return data;
    }
}

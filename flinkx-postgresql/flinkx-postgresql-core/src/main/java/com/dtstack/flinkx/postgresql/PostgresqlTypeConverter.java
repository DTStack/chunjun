/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.postgresql;

import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * The type converter for PostgreSQL database
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class PostgresqlTypeConverter implements TypeConverterInterface {

    private List<String> stringTypes = Arrays.asList("uuid","xml","cidr","inet","macaddr");

    private List<String> byteTypes = Arrays.asList("bytea","bit varying");

    private List<String> bitTypes = Collections.singletonList("bit");

    private List<String> doubleTypes = Collections.singletonList("money");

    private List<String> intTypes = Arrays.asList("int","int2","int4","int8");

    protected static List<String> STRING_TYPES = Arrays.asList("CHAR", "VARCHAR","TINYBLOB","TINYTEXT","BLOB","TEXT", "MEDIUMBLOB", "MEDIUMTEXT", "LONGBLOB", "LONGTEXT");


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
            //如果是string类型 还应该返回空字符串而不是null
            if(STRING_TYPES.contains(typeName.toUpperCase(Locale.ENGLISH))){
                return dataValue;
            }
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
            if(dataValue.contains(".")){
                dataValue =  new BigDecimal(dataValue).stripTrailingZeros().toPlainString();
            }
            data = Long.parseLong(dataValue);
        }

        return data;
    }
}

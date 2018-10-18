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

import java.util.Arrays;
import java.util.List;

/**
 * The type converter for PostgreSQL database
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class PostgresqlTypeConverter implements TypeConverterInterface {

    private List<String> stringTypes = Arrays.asList("uuid","xml","cidr","inet","macaddr");

    private List<String> byteTypes = Arrays.asList("bytea","bit varying");

    private List<String> bitTypes = Arrays.asList("bit");

    private List<String> doubleTypes = Arrays.asList("money");

    private List<String> intTypes = Arrays.asList("int","int2","int4","int8");

    @Override
    public Object convert(Object data,String typeName) {
        if (data == null){
            return null;
        }

        if(doubleTypes.contains(typeName)){
            data = Double.parseDouble(String.valueOf(data));
        } else if(bitTypes.contains(typeName)){
            //
        } else if(stringTypes.contains(typeName)){
            data = String.valueOf(data);
        } else if(byteTypes.contains(typeName)){
            data = Byte.valueOf(String.valueOf(data));
        } else if(intTypes.contains(typeName)){
            if(data instanceof String){
                data = Integer.parseInt(data.toString());
            }
        }

        return data;
    }
}

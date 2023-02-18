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
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Predicate;

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
            // According to https://www.postgresql.org/docs/current/datatype-binary.html
            // the bytea data type is corresponding to byte array (byte[]) in java.
            if (!(data instanceof byte[])) {
                // convert binary string to byte[]
                // - escape format e.g. \153\154\155\251\124 (3 octal digits and precede by backslash per byte)
                // - hex format. e.g. \xDEADBEEF (2 hex digits per byte)

                // NOTE: we suppose the given binary string is valid,
                // otherwise it makes no sense.
                if (dataValue.startsWith("\\x")) { // hex format
                    data =
                            parseBinaryString2ByteArray(
                                    dataValue.substring(2).replace(" ", ""),
                                    2,
                                    16,
                                    s -> s.length() == 2,
                                    Function.identity());
                } else if (dataValue.startsWith("\\")) { // escape format
                    data =
                            parseBinaryString2ByteArray(
                                    dataValue,
                                    4,
                                    8,
                                    s -> s.length() == 4 && s.startsWith("\\"),
                                    s -> s.replace("\\", ""));
                } else {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Invalid binary string [%s]. can not convert to bytea type.",
                                    dataValue));
                }
            }
        } else if(intTypes.contains(typeName)){
            if(dataValue.contains(".")){
                dataValue =  new BigDecimal(dataValue).stripTrailingZeros().toPlainString();
            }
            data = Long.parseLong(dataValue);
        }

        return data;
    }

    private byte[] parseBinaryString2ByteArray(
            String s,
            int numsPerGroup,
            int radix,
            Predicate<String> checker,
            Function<String, String> groupProcessor) {
        Iterable<String> it = Splitter.fixedLength(numsPerGroup).split(s);
        byte[] ret = new byte[Iterables.size(it)];
        Iterator<String> iterator = it.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            String nums = iterator.next();
            if (!checker.test(nums)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Invalid binary string [%s]. can not parse to bytea type.", s));
            }
            ret[i++] = Byte.parseByte(groupProcessor.apply(nums), radix);
        }
        return ret;
    }
}

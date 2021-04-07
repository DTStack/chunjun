/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.util;

import com.dtstack.flinkx.conf.FieldConf;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Date: 2021/04/07
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class TableUtil {

    /**
     * 获取TypeInformation
     * @param fieldList 任务参数实体类
     * @return TypeInformation
     */
    public static TypeInformation<Row> getRowTypeInformation(List<FieldConf> fieldList) {
        List<String> fieldName = fieldList.stream().map(FieldConf::getName).collect(Collectors.toList());
        if (fieldName.size() == 0) {
            return new GenericTypeInfo<>(Row.class);
        }
        return new RowTypeInfo(getTypeInformation(fieldList), fieldName.toArray(new String[0]));
    }

    /**
     * 获取TypeInformation
     * @param fieldList 任务参数实体类
     * @return TypeInformation
     */
    public static TypeInformation[] getTypeInformation(List<FieldConf> fieldList){
        Class<?>[] fieldClasses = fieldList.stream().map(FieldConf::getFieldClass).toArray(Class[]::new);
        String[] fieldTypes = fieldList.stream().map(FieldConf::getType).toArray(String[]::new);

        return IntStream.range(0, fieldClasses.length)
                .mapToObj(i -> {
                    if (fieldClasses[i].isArray()) {
                        return DataTypeUtil.convertToArray(fieldTypes[i]);
                    }
                    return TypeInformation.of(fieldClasses[i]);
                })
                .toArray(TypeInformation[]::new);
    }

}

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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.dtstack.flinkx.conf.FieldConf;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
    public static TypeInformation<RowData> getTypeInformation(List<FieldConf> fieldList) {
        List<String> fieldName = fieldList.stream().map(FieldConf::getName).collect(Collectors.toList());
        if (fieldName.size() == 0) {
            return new GenericTypeInfo<>(RowData.class);
        }
        Class<?>[] fieldClasses = fieldList.stream().map(FieldConf::getFieldClass).toArray(Class[]::new);
        DataType[] dataTypes = DataTypeUtil.getFieldTypes(Arrays.asList(fieldClasses));
        String[] fieldNames = fieldList.stream().map(FieldConf::getName).toArray(String[]::new);

        return getTypeInformation(dataTypes, fieldNames);
    }

    /**
     * 获取TypeInformation
     * @param dataTypes
     * @param fieldNames
     * @return
     */
    public static TypeInformation<RowData> getTypeInformation(DataType[] dataTypes, String[] fieldNames){
        return InternalTypeInfo.of(RowType.of(
                Arrays.stream(dataTypes)
                        .map(DataType::getLogicalType)
                        .toArray(LogicalType[]::new),
                fieldNames));
    }

}

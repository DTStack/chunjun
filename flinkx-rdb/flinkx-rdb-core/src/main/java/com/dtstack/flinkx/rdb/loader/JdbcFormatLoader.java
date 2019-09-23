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
package com.dtstack.flinkx.rdb.loader;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.util.Preconditions;

/**
 * FlinkX jdbc format loader
 *
 * Date: 2019/09/19
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class JdbcFormatLoader {

    /**
     * 类型名称
     */
    private String formatName;

    /**
     * format全限定名
     */
    private String formatClassName;

    public static final int INPUT_FORMAT = 0;
    public static final int OUTPUT_FORMAT = 1;

    private final String pkgPrefixFormat = "com.dtstack.flinkx.%s.format.%s";

    private final String INPUT_FORMAT_SUFFIX = "InputFormat";

    private final String OUTPUT_FORMAT_SUFFIX = "OutputFormat";

    /**
     * JdbcFormatLoader构造器
     * @param dataType      jdbc数据源类型
     * @param formatType    format类型：INPUT_FORMAT，OUTPUT_FORMAT
     */
    public JdbcFormatLoader(String dataType, int formatType){

        Preconditions.checkArgument(StringUtils.isNotBlank(dataType));
        Preconditions.checkArgument(formatType == INPUT_FORMAT || formatType == OUTPUT_FORMAT);

        dataType = dataType.toLowerCase();
        if(formatType == INPUT_FORMAT){
            this.formatName = dataType + INPUT_FORMAT_SUFFIX;
        }else{
            this.formatName = dataType + OUTPUT_FORMAT_SUFFIX;
        }
        this.formatClassName = String.format(pkgPrefixFormat, dataType, this.formatName.substring(0, 1).toUpperCase() + this.formatName.substring(1));
    }

    /**
     * 获取format实例对象
     * @return
     */
    public Object getFormatInstance() {
        Object format = null;
        try {
            Class clz = Class.forName(formatClassName);
            format = clz.newInstance();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("error to load " + formatClassName, e);
        } catch (Exception e) {
            throw new RuntimeException(formatClassName + "don't have no parameter constructor", e);
        }

        return format;
    }

}

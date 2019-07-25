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


package com.dtstack.flinkx.hbase.writer.function;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * @author jiangbo
 * @date 2019/7/24
 */
public class FunctionTree {

    private String columnName;

    private IFunction function;

    private List<FunctionTree> inputFunctions = Lists.newArrayList();

    private String returnValFormat;

    public String evaluate(Map<String, Object> nameValueMap){
        if(StringUtils.isNotEmpty(columnName) && MapUtils.isNotEmpty(nameValueMap)){
            return function.evaluate(nameValueMap.get(columnName));
        }

        if(CollectionUtils.isNotEmpty(inputFunctions)){
            String subFuncReturnVal = returnValFormat;
            for (FunctionTree inputFunction : inputFunctions) {
                subFuncReturnVal = subFuncReturnVal.replaceFirst("\\$\\{val}", inputFunction.evaluate(nameValueMap));
            }

            return function.evaluate(subFuncReturnVal);
        }

        return null;
    }

    public void addInputFunction(FunctionTree inputFunction){
        inputFunctions.add(inputFunction);
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public IFunction getFunction() {
        return function;
    }

    public void setFunction(IFunction function) {
        this.function = function;
    }

    public String getReturnValFormat() {
        return returnValFormat;
    }

    public void setReturnValFormat(String returnValFormat) {
        this.returnValFormat = returnValFormat;
    }
}

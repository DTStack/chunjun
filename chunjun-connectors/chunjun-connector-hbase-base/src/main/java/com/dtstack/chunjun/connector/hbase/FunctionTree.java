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

package com.dtstack.chunjun.connector.hbase;

import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class FunctionTree implements Serializable {

    private static final long serialVersionUID = 2076730868867031602L;

    private String columnName;

    private IFunction function;

    private final List<FunctionTree> inputFunctions = Lists.newArrayList();

    public String evaluate(Map<String, Object> nameValueMap) throws Exception {
        if (StringUtils.isNotEmpty(columnName) && MapUtils.isNotEmpty(nameValueMap)) {
            return function.evaluate(nameValueMap.get(columnName));
        }

        if (CollectionUtils.isNotEmpty(inputFunctions)) {
            List<String> subTaskVal = new ArrayList<>();
            for (FunctionTree inputFunction : inputFunctions) {
                subTaskVal.add(inputFunction.evaluate(nameValueMap));
            }

            return function.evaluate(StringUtils.join(subTaskVal, "_"));
        } else {
            return function.evaluate(null);
        }
    }

    public void addInputFunction(FunctionTree inputFunction) {
        inputFunctions.add(inputFunction);
    }
}

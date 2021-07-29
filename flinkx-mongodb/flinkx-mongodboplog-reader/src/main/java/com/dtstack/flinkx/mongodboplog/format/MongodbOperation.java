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
package com.dtstack.flinkx.mongodboplog.format;


import java.util.ArrayList;
import java.util.List;

/**
 * @author jiangbo
 * @date 2019/12/5
 */
public enum MongodbOperation {

    /**
     * 插入
     */
    INSERT("i"),

    /**
     * 更新
     */
    UPDATE("u"),

    /**
     * 删除
     */
    DELETE("d");

    private String internalName;

    MongodbOperation(String internalName) {
        this.internalName = internalName;
    }

    public String getInternalName() {
        return internalName;
    }

    public static List<String> getInternalNames(List<String> names) {
        List<String> internalNames = new ArrayList(names.size());
        for (String name : names) {
            MongodbOperation operation = getByName(name);
            internalNames.add(operation.getInternalName());
        }

        return internalNames;
    }

    public static MongodbOperation getByName(String name) {
        for (MongodbOperation value : MongodbOperation.values()) {
            if (value.name().equalsIgnoreCase(name)){
                return value;
            }
        }

        throw new RuntimeException("不支持的操作类型:" + name);
    }

    public static MongodbOperation getByInternalNames(String name){
        for (MongodbOperation value : MongodbOperation.values()) {
            if (value.getInternalName().equalsIgnoreCase(name)){
                return value;
            }
        }

        throw new RuntimeException("不支持的操作类型:" + name);
    }
}

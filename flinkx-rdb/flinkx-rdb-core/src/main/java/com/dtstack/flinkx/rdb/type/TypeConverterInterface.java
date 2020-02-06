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

package com.dtstack.flinkx.rdb.type;

import java.io.Serializable;

/**
 * Data type converter
 *
 * @author jiangbo
 * @date 2018/6/4 17:53
 */
public interface TypeConverterInterface extends Serializable {

    /**
     * 类型转换，将数据库数据某类型的对象转换为对应的Java基本数据对象实例
     * @param data      数据记录
     * @param typeName  数据类型
     * @return
     */
    Object convert(Object data,String typeName);

}

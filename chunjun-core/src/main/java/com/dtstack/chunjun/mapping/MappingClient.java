/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.dtstack.chunjun.mapping;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;

/**
 * 根据映射规则获取对应的目标信息
 *
 * <p>Company：www.dtstack.com.
 *
 * @author shitou
 * @date 2021/12/15
 */
public class MappingClient implements Serializable {

    private static final long serialVersionUID = 1L;

    private final NameMapping nameMapping;

    public MappingClient(NameMappingConf conf) {
        this.nameMapping = new NameMapping(conf);
    }

    public RowData map(RowData value) {
        // TODO 根据配置选择匹配方式（名称匹配或正则匹配）
        return nameMapping.map(value);
    }
}

package com.dtstack.chunjun.connector.nebula.lookup.ngql;
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


import com.dtstack.chunjun.connector.nebula.conf.NebulaConf;

import com.dtstack.chunjun.connector.nebula.utils.NebulaSchemaFamily;

import org.junit.Before;
import org.junit.Test;

/**
 * @author: gaoasi
 * @email: aschaser@163.com
 * @date: 2022/11/10 3:01 下午
 */
public class LookupNGQLBuilderTest {

    private NebulaConf nebulaConf;

    /** 返回的字段 */
    private String[] fieldNames;
    /** 过滤的字段 */
    private String[] fiterFieldNames;
    /** nebula 配置对象 */

    @Before
    public void initConf(){
        nebulaConf = new NebulaConf();
        nebulaConf.setEntityName("test");
    }

    @Test
    public void buildVertexNgql(){
        nebulaConf.setSchemaType(NebulaSchemaFamily.VERTEX);
        fieldNames = new String[]{"vid","name","age"};
        fiterFieldNames = new String[]{"vid","age"};
        String build = new LookupNGQLBuilder()
                .setNebulaConf(nebulaConf)
                .setFieldNames(fieldNames)
                .setFiterFieldNames(fiterFieldNames)
                .build();
        System.out.println(build);
    }

    @Test
    public void buildEdgeNgql(){
        nebulaConf.setSchemaType(NebulaSchemaFamily.EDGE);
        fieldNames = new String[]{"srcId","dstId","rank","name","age","gender"};
        fiterFieldNames = new String[]{"srcId","dstId","age","name","gender"};
        String build = new LookupNGQLBuilder()
                .setNebulaConf(nebulaConf)
                .setFieldNames(fieldNames)
                .setFiterFieldNames(fiterFieldNames)
                .build();
        System.out.println(build);
    }
}

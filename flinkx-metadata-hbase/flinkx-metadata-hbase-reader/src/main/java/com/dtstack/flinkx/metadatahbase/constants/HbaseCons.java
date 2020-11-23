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

package com.dtstack.flinkx.metadatahbase.constants;

import com.dtstack.flinkx.metadata.MetaDataCons;

/**
 * 属性定义
 * @author kunni@dtstack.com
 */
public class HbaseCons extends MetaDataCons {

    /**
     * json键值对中的键
     */
    public static final String KEY_NAME = "name";

    public static final String KEY_REGIONS = "regions";

    /**
     * 以下为reader需要的参数
     */
    public static final String KEY_HADOOP_CONFIG = "hadoopConfig";
}

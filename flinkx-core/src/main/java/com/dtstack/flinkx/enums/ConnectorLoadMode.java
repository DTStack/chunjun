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

package com.dtstack.flinkx.enums;

/**
 * @author chuixue
 * @create 2021-04-06 14:05
 * @description 插件加载方式spi、反射
 **/
public enum ConnectorLoadMode {
    /**
     * spi，插件在classPath下既可加载
     */
    SPI,

    /**
     * CLASSLOADER，通过指定jar的路径
     */
    CLASSLOADER,
}

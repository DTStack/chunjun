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
package com.dtstack.chunjun.config;

import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.sink.WriteMode;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.nio.charset.StandardCharsets;

@EqualsAndHashCode(callSuper = true)
@Data
public class BaseFileConfig extends CommonConfig {

    private static final long serialVersionUID = -1246776653133188377L;

    private int fromLine = 1;

    private String path;

    private String fileName;

    /** 写入模式 * */
    private String writeMode = WriteMode.APPEND.name();

    /** 压缩方式 */
    private String compress;

    private String encoding = StandardCharsets.UTF_8.name();

    private long maxFileSize = ConstantValue.STORE_SIZE_G;

    private long nextCheckRows = 5000;

    private String suffix;

    private String jobIdentifier = "";
}

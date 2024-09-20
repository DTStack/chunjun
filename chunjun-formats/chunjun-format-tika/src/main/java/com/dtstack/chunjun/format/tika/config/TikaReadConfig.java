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

package com.dtstack.chunjun.format.tika.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class TikaReadConfig implements Serializable {

    public static final String ORIGINAL_FILENAME = "_ORIGINAL_FILENAME";

    private static final long serialVersionUID = 9142075335239994317L;

    /** 是否启用tika提取 */
    private boolean useExtract = false;

    /** 内容重合度比例值 0-100 */
    private int overlapRatio = 0;

    /** 是否启动分块 */
    private boolean enableChunk = false;

    /** 分块大小 */
    private int chunkSize = -1;

    public boolean isEnableChunk() {
        return chunkSize > 0 ? true : false;
    }
}

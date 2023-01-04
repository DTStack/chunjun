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

package com.dtstack.chunjun.cdc;

import com.dtstack.chunjun.cdc.config.CacheConfig;
import com.dtstack.chunjun.cdc.config.DDLConfig;

import lombok.Data;

import java.io.Serializable;

@Data
public class CdcConfig implements Serializable {

    private static final long serialVersionUID = -7411507910553457983L;

    /** whether skip ddl statement or not. */
    private boolean skipDDL = true;

    /** worker的核心线程数 */
    private int workerNum = 2;

    /** worker遍历队列时的步长 */
    private int workerSize = 3;

    /** worker线程池的最大容量 */
    private int workerMax = 3;

    private int bathSize = 1000;

    private long maxBytes = 1024 * 1024 * 1024;

    private long cacheTimeout = 60;

    private DDLConfig ddl;

    private CacheConfig cache;
}

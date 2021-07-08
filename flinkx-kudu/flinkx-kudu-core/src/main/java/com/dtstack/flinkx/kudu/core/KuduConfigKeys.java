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


package com.dtstack.flinkx.kudu.core;

/**
 * @author jiangbo
 * @date 2019/8/12
 */
public class KuduConfigKeys {

    public final static String KEY_MASTER_ADDRESSES = "masterAddresses";
    public final static String KEY_AUTHENTICATION = "authentication";
    public final static String KEY_PRINCIPAL = "principal";
    public final static String KEY_KEYTABFILE = "keytabFile";
    public final static String KEY_WORKER_COUNT = "workerCount";
    public final static String KEY_BOSS_COUNT = "bossCount";
    public final static String KEY_OPERATION_TIMEOUT = "operationTimeout";
    public final static String KEY_QUERY_TIMEOUT = "queryTimeout";
    public final static String KEY_ADMIN_OPERATION_TIMEOUT = "adminOperationTimeout";
    public final static String KEY_TABLE = "table";
    public final static String KEY_READ_MODE = "readMode";
    public final static String KEY_FLUSH_MODE = "flushMode";
    public final static String KEY_FILTER = "where";
    public final static String KEY_BATCH_SIZE_BYTES = "batchSizeBytes";
}

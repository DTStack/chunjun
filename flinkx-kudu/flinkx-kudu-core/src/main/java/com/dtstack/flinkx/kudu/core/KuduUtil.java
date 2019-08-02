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

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduClient;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;

/**
 * @author jiangbo
 * @date 2019/7/31
 */
public class KuduUtil {

    public static KuduClient getKuduClient(KuduConfig config) throws IOException,InterruptedException {
        if(config.getOpenKerberos()){
            UserGroupInformation.loginUserFromKeytab(config.getUser(), config.getKeytabPath());
            return UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<KuduClient>() {
                @Override
                public KuduClient run() throws Exception {
                    return getKuduClientInternal(config);
                }
            });
        } else {
            return getKuduClientInternal(config);
        }
    }

    private static KuduClient getKuduClientInternal(KuduConfig config) {
        return new AsyncKuduClient.AsyncKuduClientBuilder(Arrays.asList(config.getMasterAddresses().split(",")))
                .workerCount(config.getWorkerCount())
                .bossCount(config.getBossCount())
                .defaultAdminOperationTimeoutMs(config.getAdminOperationTimeout())
                .defaultOperationTimeoutMs(config.getOperationTimeout())
                .build()
                .syncClient();
    }
}

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
package com.dtstack.flinkx.client.local;

import com.dtstack.flinkx.Main;
import com.dtstack.flinkx.client.ClusterClientHelper;
import com.dtstack.flinkx.client.JobDeployer;

import org.apache.flink.client.program.ClusterClient;

/**
 * @program: flinkx
 * @author: xiuzhu
 * @create: 2021/05/31
 */
public class LocalClusterClientHelper implements ClusterClientHelper {
    @Override
    public ClusterClient submit(JobDeployer jobDeployer) throws Exception {
        String[] args = jobDeployer.getProgramArgs().toArray(new String[0]);
        Main.main(args);
        return null;
    }
}

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

package com.dtstack.chunjun.connector.test.standalone.stream;

import com.dtstack.chunjun.connector.entity.JobAccumulatorResult;
import com.dtstack.chunjun.connector.test.utils.ChunjunFlinkStandaloneTestEnvironment;

import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;

public class FlinkStandaloneStreamE2eITCase extends ChunjunFlinkStandaloneTestEnvironment {

    @Test
    public void testStream() throws Exception {
        submitSyncJobOnStandLone(
                ChunjunFlinkStandaloneTestEnvironment.CHUNJUN_HOME
                        + "/chunjun-examples/json/stream/stream.json");
        JobAccumulatorResult jobAccumulatorResult = waitUntilJobFinished(Duration.ofMinutes(30));

        Assert.assertEquals(jobAccumulatorResult.getNumRead(), 30);
    }
}

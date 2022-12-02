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

package com.dtstack.chunjun.connector.nebula.splitters.creator;

import com.dtstack.chunjun.connector.nebula.config.NebulaConfig;
import com.dtstack.chunjun.connector.nebula.splitters.NebulaInputSplitter;

import java.util.ArrayList;
import java.util.List;

public class BaseSplitResponsibility {

    private final List<BaseSplitResponsibility> splitResponsibilities = new ArrayList<>();

    public BaseSplitResponsibility(Boolean init) {
        if (init) {
            splitResponsibilities.add(new EqualSplitCreator(false));
            splitResponsibilities.add(new SplitNumGTPartNumSplitCtreator(false));
            splitResponsibilities.add(new SplitNumLTPartNumSplitCreator(false));
        }
    }

    public void createSplit(
            int minNumSplits,
            int partNum,
            NebulaInputSplitter[] nebulaInputSplitters,
            List<Integer> spaceParts,
            NebulaConfig nebulaConfig) {
        for (BaseSplitResponsibility splitResponsibility : splitResponsibilities) {
            splitResponsibility.createSplit(
                    minNumSplits, partNum, nebulaInputSplitters, spaceParts, nebulaConfig);
        }
    }
}

package com.dtstack.chunjun.connector.nebula.splitters.creator;
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

import com.dtstack.chunjun.connector.nebula.conf.NebulaConf;
import com.dtstack.chunjun.connector.nebula.splitters.NebulaInputSplitter;

import java.util.List;

/**
 * @author: gaoasi
 * @email: aschaser@163.com
 * @date: 2022/11/3 5:23 下午
 */
public class EqualSplitCreator extends BaseSplitResponsibility {

    public EqualSplitCreator(Boolean init) {
        super(init);
    }

    @Override
    public void createSplit(
            int minNumSplits,
            int partNum,
            NebulaInputSplitter[] nebulaInputSplitters,
            List<Integer> spaceParts,
            NebulaConf nebulaConf) {
        if (minNumSplits == partNum) {
            for (int i = 0; i < spaceParts.size(); i++) {
                nebulaInputSplitters[i] =
                        new NebulaInputSplitter(
                                i,
                                minNumSplits,
                                nebulaConf.getStart(),
                                nebulaConf.getEnd(),
                                nebulaConf.getInterval());
                nebulaInputSplitters[i].parts.push(spaceParts.get(i));
            }
        }
    }
}

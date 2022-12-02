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

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class BaseSplitResponsibilityTest {

    private int minNumSplits;
    private int partNum;
    private NebulaInputSplitter[] nebulaInputSplitters;
    private List<Integer> spaceParts;
    private NebulaConfig nebulaConfig;

    @Before
    public void initConf() {
        nebulaConfig = new NebulaConfig();
        nebulaConfig.setStart(100L);
        nebulaConfig.setEnd(200L);
        nebulaConfig.setInterval(10L);
    }

    public void eq() {
        minNumSplits = 3;
        partNum = 3;
        spaceParts = Arrays.asList(1, 2, 3);
        nebulaInputSplitters = new NebulaInputSplitter[minNumSplits];
    }

    public void gt() {
        minNumSplits = 5;
        partNum = 3;
        spaceParts = Arrays.asList(1, 2, 3);
        nebulaInputSplitters = new NebulaInputSplitter[minNumSplits];
    }

    public void lt() {
        minNumSplits = 3;
        partNum = 7;
        spaceParts = Arrays.asList(1, 2, 3, 4, 5, 6, 7);
        nebulaInputSplitters = new NebulaInputSplitter[minNumSplits];
    }

    @Test
    public void testEqualSplitCreate() {
        eq();
        BaseSplitResponsibility baseSplitResponsibility = new BaseSplitResponsibility(true);
        System.out.println(nebulaConfig.getInterval());
        System.out.println(nebulaConfig.getStart());
        System.out.println(nebulaConfig.getEnd());
        baseSplitResponsibility.createSplit(
                minNumSplits, partNum, nebulaInputSplitters, spaceParts, nebulaConfig);
        print();
    }

    @Test
    public void testGTSplitCreate() {
        gt();
        BaseSplitResponsibility baseSplitResponsibility = new BaseSplitResponsibility(true);
        baseSplitResponsibility.createSplit(
                minNumSplits, partNum, nebulaInputSplitters, spaceParts, nebulaConfig);
        print();
    }

    @Test
    public void testLTSplitCreate() {
        lt();
        BaseSplitResponsibility baseSplitResponsibility = new BaseSplitResponsibility(true);
        baseSplitResponsibility.createSplit(
                minNumSplits, partNum, nebulaInputSplitters, spaceParts, nebulaConfig);
        print();
    }

    public void print() {
        for (NebulaInputSplitter nebulaInputSplitter : nebulaInputSplitters) {
            System.out.println(nebulaInputSplitter.toString());
        }
    }
}

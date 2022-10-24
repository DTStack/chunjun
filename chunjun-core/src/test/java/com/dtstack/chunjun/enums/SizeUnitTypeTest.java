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

package com.dtstack.chunjun.enums;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SizeUnitTypeTest {

    @Test
    @DisplayName("Should set the name when the name is not null")
    public void setNameWhenNameIsNotNull() {
        SizeUnitType sizeUnitType = SizeUnitType.B;
        sizeUnitType.setName("B");
        assertEquals("B", sizeUnitType.getName());
    }

    @Test
    @DisplayName("Should return the correct value when source is bigger than target")
    public void covertUnitWhenSourceIsBiggerThanTarget() {
        String result = SizeUnitType.covertUnit(SizeUnitType.MB, SizeUnitType.KB, 1024L);
        assertEquals("1048576", result);
    }

    @Test
    @DisplayName("Should return the correct value when source is smaller than target")
    public void covertUnitWhenSourceIsSmallerThanTarget() {
        String result = SizeUnitType.covertUnit(SizeUnitType.B, SizeUnitType.KB, 1024L);
        assertEquals("1.00", result);
    }

    @Test
    @DisplayName("Should set the code when the code is positive")
    public void setCodeWhenCodeIsPositive() {
        SizeUnitType sizeUnitType = SizeUnitType.B;
        sizeUnitType.setCode(1);
        assertEquals(1, sizeUnitType.getCode());
    }

    @Test
    @DisplayName("Should return 0 when the size is 0")
    public void readableFileSizeWhenSizeIs0ThenReturn0() {
        assertEquals("0", SizeUnitType.readableFileSize(0));
    }

    @Test
    @DisplayName("Should return 1b when the size is 1")
    public void readableFileSizeWhenSizeIs1ThenReturn1B() {
        assertEquals("1B", SizeUnitType.readableFileSize(1));
    }
}

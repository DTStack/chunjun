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

class ECacheContentTypeTest {

    @Test
    @DisplayName("Should return 0 when the type is missval")
    public void getTypeWhenTypeIsMissVal() {
        ECacheContentType eCacheContentType = ECacheContentType.MissVal;
        assertEquals(0, eCacheContentType.getType());
    }

    @Test
    @DisplayName("Should return 1 when the type is singleline")
    public void getTypeWhenTypeIsSingleLine() {
        ECacheContentType eCacheContentType = ECacheContentType.SingleLine;
        assertEquals(1, eCacheContentType.getType());
    }
}

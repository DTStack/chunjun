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

package com.dtstack.chunjun.decoder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonDecoderTest {

    private JsonDecoder jsonDecoder;

    @BeforeEach
    void setUp() {
        jsonDecoder = new JsonDecoder();
    }

    @Test
    @DisplayName("Should return a map with the message when the message is valid json")
    void decodeWhenMessageIsValidJsonThenReturnMapWithMessage() {
        String message = "{\"message\":\"test\"}";
        Map<String, Object> result = jsonDecoder.decode(message);
        assertEquals(1, result.size());
        assertEquals("test", result.get("message"));
    }

    @Test
    @DisplayName("Should return a map with the message when the message is not valid json")
    void decodeWhenMessageIsNotValidJsonThenReturnMapWithMessage() {
        String message = "not valid json";
        Map<String, Object> result = jsonDecoder.decode(message);
        assertEquals(1, result.size());
        assertEquals(message, result.get("message"));
    }
}

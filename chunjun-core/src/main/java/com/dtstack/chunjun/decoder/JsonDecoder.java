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

package com.dtstack.chunjun.decoder;

import com.dtstack.chunjun.util.JsonUtil;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

@Slf4j
@NoArgsConstructor
public class JsonDecoder implements IDecode, Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(JsonDecoder.class);

    private static final String KEY_MESSAGE = "message";
    private boolean addMessage;

    public JsonDecoder(boolean addMessage) {
        this.addMessage = addMessage;
    }

    @Override
    public Map<String, Object> decode(final String message) {
        try {
            Map<String, Object> event = JsonUtil.toObject(message, JsonUtil.MAP_TYPE_REFERENCE);
            if (addMessage && !event.containsKey(KEY_MESSAGE)) {
                event.put(KEY_MESSAGE, message);
            }
            return event;
        } catch (Exception e) {
            LOG.error(e.getMessage());
            return Collections.singletonMap(KEY_MESSAGE, message);
        }
    }
}

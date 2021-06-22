/*
 *    Copyright 2021 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.dtstack.flinkx.connector.api;

import org.apache.kafka.connect.data.Schema;

public class ChangeEntry {

    private Object value;
    private Schema.Type type;

    public ChangeEntry(Object value, Schema.Type type) {
        this.value = value;
        this.type = type;
    }

    public Object getValue() {
        return value;
    }

    public Schema.Type getType() {
        return type;
    }

    @Override
    public String toString() {
        return "ChangeEntry{" +
                "value=" + value +
                ", type=" + type +
                '}';
    }
}

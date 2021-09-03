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

package com.dtstack.flinkx.lookup.cache;

import com.dtstack.flinkx.enums.ECacheContentType;

/**
 * Reason: Date: 2018/9/10 Company: www.dtstack.com
 *
 * @author xuchao
 */
public class CacheObj {

    private ECacheContentType type;

    private Object content;

    private CacheObj(ECacheContentType type, Object content) {
        this.type = type;
        this.content = content;
    }

    public static CacheObj buildCacheObj(ECacheContentType type, Object content) {
        return new CacheObj(type, content);
    }

    public ECacheContentType getType() {
        return type;
    }

    public void setType(ECacheContentType type) {
        this.type = type;
    }

    public Object getContent() {
        return content;
    }

    public void setContent(Object content) {
        this.content = content;
    }
}

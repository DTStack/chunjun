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
package com.dtstack.flinkx.restapi.common;

import java.util.Map;

/**
 * httprequestApi
 *
 * @author by dujie@dtstack.com
 * @Date 2020/9/25
 */
public abstract class httprequestApi<R> {

    abstract R execute();


    abstract void getHttpRequest();

    public static class Httprequest {
        private Map<String, Object> body;
        private Map<String, Object> param;
        private Map<String, String> header;

        public Httprequest buildBody(Map<String, Object> body) {
            this.body = body;
            return this;
        }

        public Httprequest buildParam(Map<String, Object> param) {
            this.param = param;
            return this;
        }

        public Httprequest buildHeader(Map<String, String> header) {
            this.header = header;
            return this;
        }

        @Override
        public String toString() {
            return "Httprequest{" +
                    "body:" + body +
                    ", param:" + param +
                    ", header:" + header +
                    '}';
        }

        public Map<String, Object> getBody() {
            return body;
        }

        public Map<String, Object> getParam() {
            return param;
        }

        public Map<String, String> getHeader() {
            return header;
        }
    }
}

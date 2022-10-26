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

package com.dtstack.chunjun.interceptor;

import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogInterceptor implements Interceptor {

    private static final Logger LOG = LoggerFactory.getLogger(LogInterceptor.class);

    @Override
    public void init(Configuration configuration) {
        if (LOG.isDebugEnabled()) {
            LOG.info("configuration: ");
            configuration
                    .keySet()
                    .forEach(
                            key ->
                                    LOG.debug(
                                            "key : {} -> value : {}",
                                            key,
                                            configuration.toMap().get(key)));
        }
    }

    @Override
    public void pre(Context context) {
        if (LOG.isDebugEnabled()) {
            for (String key : context) {
                LOG.debug("context key : {} -> value : {} ", key, context.get(key));
            }
        }
    }

    @Override
    public void post(Context context) {
        if (LOG.isDebugEnabled()) {
            for (String key : context) {
                LOG.debug("context key : {} -> value : {} ", key, context.get(key));
            }
        }
    }
}

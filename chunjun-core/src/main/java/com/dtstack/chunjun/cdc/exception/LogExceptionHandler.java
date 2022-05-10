/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.cdc.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/8 星期三
 */
public class LogExceptionHandler implements Thread.UncaughtExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(LogExceptionHandler.class);

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        LOG.error(
                String.format(
                        "An error occurred during the sending data. thread name : [%s]",
                        t.getName()),
                e);
    }
}

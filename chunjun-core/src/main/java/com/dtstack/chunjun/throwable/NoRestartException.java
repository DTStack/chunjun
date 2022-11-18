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

package com.dtstack.chunjun.throwable;

import org.apache.flink.runtime.throwable.ThrowableAnnotation;
import org.apache.flink.runtime.throwable.ThrowableType;

@ThrowableAnnotation(ThrowableType.NonRecoverableError)
public class NoRestartException extends ChunJunRuntimeException {

    private static final long serialVersionUID = 8612872830721453715L;

    public NoRestartException(String message) {
        super(message);
    }

    public NoRestartException(Throwable throwable) {
        super(throwable);
    }

    public NoRestartException(String message, Throwable throwable) {
        super(message, throwable);
    }
}

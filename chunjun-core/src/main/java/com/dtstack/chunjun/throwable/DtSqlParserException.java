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

import static com.dtstack.chunjun.util.DtStringUtil.addLineNumber;

/**
 * @author chuixue
 * @create 2021-05-13 14:30
 * @description sql解析异常
 */
public class DtSqlParserException extends RuntimeException {
    public DtSqlParserException(String sql, String message, Throwable e) {
        super(
                "\n----------sql start---------\n"
                        + addLineNumber(sql)
                        + "\n----------sql end--------- \n\n"
                        + message,
                e);
    }
}

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

package com.dtstack.chunjun.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SplitterTest {

    @Test
    public void testSplitEscaped() {
        Splitter splitter = new Splitter(';');

        String str1 = "show '\\\\'; \ndesc `'dd`";
        List<String> sqlStmts1 = splitter.splitEscaped(str1);
        Assert.assertEquals("show '\\\\'", sqlStmts1.get(0));
        Assert.assertEquals(" \ndesc `'dd`", sqlStmts1.get(1));

        String str2 = "WHERE a = ';\\\\；' b = \";'\"; \ndesc `'dd`";
        List<String> sqlStmts2 = splitter.splitEscaped(str2);
        Assert.assertEquals("WHERE a = ';\\\\；' b = \";'\"", sqlStmts2.get(0));
        Assert.assertEquals(" \ndesc `'dd`", sqlStmts2.get(1));

        String str3 = "WHERE password = ';\\';'; \ndesc `'dd`";
        List<String> sqlStmts3 = splitter.splitEscaped(str3);
        Assert.assertEquals("WHERE password = ';\\';'", sqlStmts3.get(0));
        Assert.assertEquals(" \ndesc `'dd`", sqlStmts3.get(1));
    }
}

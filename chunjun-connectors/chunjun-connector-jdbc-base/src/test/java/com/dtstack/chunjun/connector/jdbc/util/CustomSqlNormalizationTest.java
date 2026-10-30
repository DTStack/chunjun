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
package com.dtstack.chunjun.connector.jdbc.util;

import org.junit.Assert;
import org.junit.Test;

public class CustomSqlNormalizationTest {
    @Test
    public void removesOnlyTerminalSeparators() {
        Assert.assertEquals("SELECT 1 -- tail\n", SqlUtil.prepareCustomSql("SELECT 1; -- tail"));
        Assert.assertEquals(
                "SELECT 1 /* tail */ \n", SqlUtil.prepareCustomSql("SELECT 1; /* tail */ ;"));
        Assert.assertEquals("SELECT 1 \n# tail\n", SqlUtil.prepareCustomSql("SELECT 1; \n# tail"));
        Assert.assertEquals(
                "SELECT 1; SELECT 2\n", SqlUtil.prepareCustomSql("SELECT 1; SELECT 2;"));
    }

    @Test
    public void preservesQuotedContentsAndEscapes() {
        for (String query :
                new String[] {
                    "SELECT 'a;--b'", "SELECT 'it''s;#'", "SELECT \"a;--\"",
                    "SELECT `a;--`", "SELECT [a;--]]b]", "SELECT 'a\\';--b'",
                    "SELECT $$;--$$", "SELECT $tag$;/*x*/$tag$"
                }) {
            Assert.assertEquals(query + '\n', SqlUtil.prepareCustomSql(query + ';'));
        }
    }

    @Test
    public void preservesNestedBlockComments() {
        Assert.assertEquals(
                "SELECT 1 /* outer /* inner ; */ tail */\n",
                SqlUtil.prepareCustomSql("SELECT 1; /* outer /* inner ; */ tail */"));
    }

    @Test
    public void leavesUnclosedTokensUnchanged() {
        for (String query : new String[] {"SELECT 1; /*", "SELECT 'x;", "SELECT $tag$x;"}) {
            Assert.assertEquals(query + '\n', SqlUtil.prepareCustomSql(query));
        }
        Assert.assertNull(SqlUtil.prepareCustomSql(null));
        Assert.assertEquals(" ", SqlUtil.prepareCustomSql(" "));
    }
}

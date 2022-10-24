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

package com.dtstack.chunjun.security;

import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KerberosConfigTest {

    @Test
    public void testKerberosConfigAllNotSet() {
        KerberosConfig kerberosConfig = new KerberosConfig(null, null, null);
        assertFalse(kerberosConfig.enableKrb);
    }

    @Test
    public void testKerberosConfigAllSet() {
        KerberosConfig kerberosConfig = new KerberosConfig("principal", "keytab", "krb5conf");
        assertTrue(kerberosConfig.enableKrb);
    }

    @Test
    public void testKerberosConfigNotAllSet() {
        ChunJunRuntimeException thrown =
                assertThrows(
                        ChunJunRuntimeException.class,
                        () -> new KerberosConfig("principal", "keytab", null),
                        "Expected KerberosConfig#init to throw, but it didn't");
        assertEquals(
                "Missing kerberos parameter! all kerberos params must be set, or all kerberos params are not set",
                thrown.getMessage());
    }
}

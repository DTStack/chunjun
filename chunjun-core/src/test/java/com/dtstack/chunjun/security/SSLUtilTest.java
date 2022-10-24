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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SSLUtilTest {

    @TempDir static File tmpFile;

    @Test
    public void testCheckFileExistsIfPathIsFile() throws IOException {
        String path = tmpFile.getPath() + "/" + UUID.randomUUID();
        File newFile = new File(path);
        newFile.createNewFile();
        assertDoesNotThrow(() -> SSLUtil.checkFileExists(newFile.getPath()));
    }

    @Test
    public void testCheckFileExistsIfPathIsDir() {
        RuntimeException thrown =
                assertThrows(
                        RuntimeException.class,
                        () -> SSLUtil.checkFileExists(tmpFile.getPath()),
                        "Expected checkFileExists to throw, but it didn't");
        assertTrue(thrown.getMessage().contains("is a directory."));
    }

    @Test
    public void testCheckFileExistsIfPathIsNotExisted() {
        RuntimeException thrown =
                assertThrows(
                        RuntimeException.class,
                        () -> SSLUtil.checkFileExists(tmpFile.getPath() + "/" + UUID.randomUUID()),
                        "Expected checkFileExists to throw, but it didn't");
        assertTrue(thrown.getMessage().contains(" not exists."));
    }

    @Test
    public void testFileExists() throws IOException {
        String path = tmpFile.getPath() + "/" + UUID.randomUUID();

        assertFalse(SSLUtil.fileExists(path));

        File newFile = new File(path);
        newFile.createNewFile();
        assertTrue(SSLUtil.fileExists(path));
    }
}

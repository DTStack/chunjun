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
package com.dtstack.chunjun.connector.oracle.converter;

import oracle.sql.BLOB;

import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.SQLException;

public class ConvertUtil {

    public static byte[] toByteArray(BLOB fromBlob) throws SQLException {
        int blobLength = (int) fromBlob.length();
        byte[] blobAsBytes = fromBlob.getBytes(1, blobLength);
        fromBlob.free();
        return blobAsBytes;
    }

    public static String convertClob(Clob clob) throws SQLException, IOException {
        StringBuilder buffer = new StringBuilder();
        try (Reader r = clob.getCharacterStream()) {
            int ch;
            while ((ch = r.read()) != -1) {
                buffer.append((char) ch);
            }
        }
        clob.free();
        return buffer.toString();
    }
}

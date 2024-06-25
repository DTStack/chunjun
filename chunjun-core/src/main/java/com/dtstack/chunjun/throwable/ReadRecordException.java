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

import java.io.IOException;

/** The Exception describing errors when read a record */
public class ReadRecordException extends IOException {

    private static final long serialVersionUID = 453087894656079820L;
    private final int colIndex;
    private final Object rowData;

    public ReadRecordException(String message, Throwable cause, int colIndex, Object rowData) {
        super(message, cause);
        this.colIndex = colIndex;
        this.rowData = rowData;
    }

    public ReadRecordException(String message, Throwable cause, int colIndex, String rowData) {
        super(message, cause);
        this.colIndex = colIndex;
        this.rowData = rowData;
    }

    public int getColIndex() {
        return colIndex;
    }

    public Object getRowData() {
        return rowData;
    }

    public ReadRecordException(String message, Throwable cause) {
        this(message, cause, -1, null);
    }

    @Override
    public String toString() {
        return super.toString() + "\n" + getCause().toString();
    }
}

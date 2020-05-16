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

package com.dtstack.flinkx.exception;

import org.apache.flink.types.Row;

/**
 * The Exception describing errors when writing a record
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class WriteRecordException extends Exception {

    private int colIndex = -1;
    private Row row;

    public int getColIndex() {
        return colIndex;
    }

    public Row getRow() {
        return row;
    }

    public WriteRecordException(String message, Throwable cause, int colIndex, Row row) {
        super(message, cause);
        this.colIndex = colIndex;
        this.row = row;
    }

    public WriteRecordException(String message, Throwable cause) {
        this(message, cause, -1, null);
    }

    @Override
    public String toString() {
        return super.toString() + "\n" + getCause().toString();
    }

}

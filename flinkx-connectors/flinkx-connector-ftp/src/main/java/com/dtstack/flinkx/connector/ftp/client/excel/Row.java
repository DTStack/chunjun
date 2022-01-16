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

package com.dtstack.flinkx.connector.ftp.client.excel;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/** @author by dujie @Description @Date 2021/12/20 */
public class Row implements Serializable {

    private static final long serialVersionUID = 1L;

    private String[] data;
    private int sheetIndex;
    private int rowIndex;
    private boolean end;

    public Row(String[] data, int sheetIndex, int rowIndex, boolean end) {
        this.data = data;
        this.sheetIndex = sheetIndex;
        this.rowIndex = rowIndex;
        this.end = end;
    }

    public Row() {}

    public String[] getData() {
        return data;
    }

    public void setData(String[] data) {
        this.data = data;
    }

    public int getSheetIndex() {
        return sheetIndex;
    }

    public void setSheetIndex(int sheetIndex) {
        this.sheetIndex = sheetIndex;
    }

    public int getRowIndex() {
        return rowIndex;
    }

    public void setRowIndex(int rowIndex) {
        this.rowIndex = rowIndex;
    }

    public boolean isEnd() {
        return end;
    }

    public void setEnd(boolean end) {
        this.end = end;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Row row = (Row) o;
        return sheetIndex == row.sheetIndex
                && rowIndex == row.rowIndex
                && end == row.end
                && Arrays.equals(data, row.data);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(sheetIndex, rowIndex, end);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public String toString() {
        return "Row{"
                + "data="
                + Arrays.toString(data)
                + ", sheetIndex="
                + sheetIndex
                + ", rowIndex="
                + rowIndex
                + ", end="
                + end
                + '}';
    }
}

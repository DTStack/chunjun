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

package com.dtstack.chunjun.metrics;

import com.dtstack.chunjun.cdc.DdlRowData;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.data.RowData;

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;

import java.util.Arrays;
import java.util.stream.Collectors;

public abstract class RowSizeCalculator<T> {

    public abstract long getObjectSize(T object);

    public static RowSizeCalculator getRowSizeCalculator(
            String calculatorType, boolean useAbstractColumn) {
        if (useAbstractColumn) {
            return new SyncCalculator();
        }
        switch (CalculatorType.getCalculatorTypeByName(calculatorType)) {
            case TO_STRING_CALCULATOR:
                return new RowToStringCalculator();
            case OBJECT_SIZE_CALCULATOR:
                return getRowSizeCalculator();
            case UNDO_CALCULATOR:
                return new UndoCalculator();
            default:
                throw new UnsupportedTypeException(calculatorType);
        }
    }

    /**
     * if jdk support,use {@link jdk.nashorn.internal.ir.debug.ObjectSizeCalculator} else use
     * toString().getBytes().length
     *
     * <p>ObjectSizeCalculator is in jre/lib/ext/nashorn.jar,In order to be able to determine
     * correctly in different JDK, we call it directly once
     *
     * @return RowSizeCalculator
     */
    public static RowSizeCalculator getRowSizeCalculator() {
        try {
            ObjectSizeCalculator.getObjectSize("");
            return new RowObjectSizeCalculator();
        } catch (Throwable e) {
            return new RowToStringCalculator();
        }
    }

    static class RowObjectSizeCalculator extends RowSizeCalculator<Object> {
        @Override
        public long getObjectSize(Object object) {
            return ObjectSizeCalculator.getObjectSize(object);
        }
    }

    static class RowToStringCalculator extends RowSizeCalculator<Object> {
        @Override
        public long getObjectSize(Object object) {
            return object.toString().getBytes().length;
        }
    }

    static class UndoCalculator extends RowSizeCalculator<Object> {
        @Override
        public long getObjectSize(Object object) {
            return 0;
        }
    }

    static class SyncCalculator extends RowSizeCalculator<RowData> {
        @Override
        public long getObjectSize(RowData rowData) {
            if (rowData instanceof ColumnRowData) {
                return ((ColumnRowData) rowData).getByteSize();
            } else if (rowData instanceof DdlRowData) {
                return ((DdlRowData) rowData).getByteSize();
            }
            throw new RuntimeException(
                    "not support get rowSize for " + rowData.getClass().getName());
        }
    }

    public enum CalculatorType {
        TO_STRING_CALCULATOR("toStringCalculator"),
        OBJECT_SIZE_CALCULATOR("objectSizeCalculator"),
        UNDO_CALCULATOR("undoCalculator"),
        SYNC_CALCULATOR("syncCalculator");

        private String typeName;

        CalculatorType(String typeName) {
            this.typeName = typeName;
        }

        public String getTypeName() {
            return typeName;
        }

        public void setTypeName(String typeName) {
            this.typeName = typeName;
        }

        public static CalculatorType getCalculatorTypeByName(String name) {
            for (CalculatorType calculatorType : CalculatorType.values()) {
                if (name.equalsIgnoreCase(calculatorType.typeName)) {
                    return calculatorType;
                }
            }
            throw new ChunJunRuntimeException(
                    String.format(
                            "ChunJun CalculatorType only support one of %s",
                            Arrays.stream(CalculatorType.values())
                                    .map(CalculatorType::getTypeName)
                                    .collect(Collectors.toList())));
        }
    }
}

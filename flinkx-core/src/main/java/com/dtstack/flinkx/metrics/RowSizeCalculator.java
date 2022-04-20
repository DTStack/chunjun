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

package com.dtstack.flinkx.metrics;

import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.throwable.UnsupportedTypeException;

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;

import java.util.Arrays;
import java.util.stream.Collectors;

/** @author liuliu 2022/4/13 */
public abstract class RowSizeCalculator {

    public abstract long getObjectSize(Object object);

    public static RowSizeCalculator getRowSizeCalculator(String calculatorType) {
        if (calculatorType == null) return getRowSizeCalculator();
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
     * @return RowSizeCalculator
     */
    public static RowSizeCalculator getRowSizeCalculator() {
        String vmName = System.getProperty("java.vm.name");
        String dataModel = System.getProperty("sun.arch.data.model");
        if (vmName != null
                && (vmName.startsWith("OpenJDK ") || vmName.startsWith("Java HotSpot(TM) "))) {
            if ("32".equals(dataModel) || "64".equals(dataModel)) {
                return new RowObjectSizeCalculator();
            }
        }
        return new RowToStringCalculator();
    }

    static class RowObjectSizeCalculator extends RowSizeCalculator {
        @Override
        public long getObjectSize(Object object) {
            return ObjectSizeCalculator.getObjectSize(object);
        }
    }

    static class RowToStringCalculator extends RowSizeCalculator {
        @Override
        public long getObjectSize(Object object) {
            return object.toString().getBytes().length;
        }
    }

    static class UndoCalculator extends RowSizeCalculator {
        @Override
        public long getObjectSize(Object object) {
            return 0;
        }
    }

    public enum CalculatorType {
        TO_STRING_CALCULATOR("toStringCalculator"),
        OBJECT_SIZE_CALCULATOR("objectSizeCalculator"),
        UNDO_CALCULATOR("undoCalculator");

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
            throw new FlinkxRuntimeException(
                    String.format(
                            "FlinkX CalculatorType only one of %s",
                            Arrays.stream(CalculatorType.values())
                                    .map(CalculatorType::getTypeName)
                                    .collect(Collectors.toList())));
        }
    }
}

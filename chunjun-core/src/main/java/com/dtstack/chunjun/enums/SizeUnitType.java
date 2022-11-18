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

package com.dtstack.chunjun.enums;

import java.text.DecimalFormat;

public enum SizeUnitType {

    /** B */
    B(1, "B"),

    /** KB */
    KB(1024, "KB"),

    /** MB */
    MB(1024 * 1024, "MB"),

    /** GB */
    GB(1024 * 1024 * 1024, "GB");

    private int code;

    private String name;

    /** 小单位转大单位保留两位小数 */
    private static final DecimalFormat df = new DecimalFormat("0.00"); // 格式化小

    SizeUnitType(int code, String name) {
        this.code = code;
        this.name = name;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * 不同单位类型转换
     *
     * @param source
     * @param target
     * @param value
     * @return
     */
    public static String covertUnit(SizeUnitType source, SizeUnitType target, Long value) {
        // 大单位转小单位
        if (source.getCode() > target.getCode()) {
            return String.valueOf(value * source.getCode() / target.getCode());
        }
        // 小单位转大单位
        return df.format((float) value * source.getCode() / target.getCode());
    }

    /**
     * 字节单位自动转换
     *
     * @param size byte
     * @return
     */
    public static String readableFileSize(long size) {
        if (size <= 0) return "0";
        final String[] units = new String[] {"B", "KB", "MB", "GB", "TB"};
        int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
        return new DecimalFormat("#,##0.#").format(size / Math.pow(1024, digitGroups))
                + units[digitGroups];
    }
}

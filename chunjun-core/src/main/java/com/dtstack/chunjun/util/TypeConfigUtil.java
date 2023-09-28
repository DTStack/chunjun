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

package com.dtstack.chunjun.util;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.commons.lang3.StringUtils;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TypeConfigUtil {
    //    private static final Pattern COMPLEX_PATTERN =
    // Pattern.compile("(?<type1>\\D+)(\\(\\s*(?<precision1>\\d)\\s*\\))?(?<type2>\\D+)(\\(\\s*(?<scale1>\\d)\\s*\\))?(\\(\\s*(?<precision2>\\d)\\s*(,\\s*(?<scale2>\\d)\\s*)?\\))?");
    private static final Pattern COMPLEX_PATTERN =
            Pattern.compile(
                    "(?<prefixType>[A-Za-z0-9\\s]+)(\\(\\s*(?<precision>\\d+)\\s*\\))?(?<suffixType>[A-Za-z0-9\\s]+)(\\(\\s*(?<scale>\\d+)\\s*\\))?");

    private static final Pattern COMPLEX_PATTERN_2 =
            Pattern.compile(
                    "(?<prefixType>[A-Za-z0-9\\s]+)(\\(\\s*(?<precision>\\d+)\\s*(,\\s*(?<scale>\\d+)\\s*)?\\))(?<suffixType>[A-Za-z0-9\\s]+)");

    private static final Pattern NORMAL_PATTERN =
            Pattern.compile(
                    "(?<type>[_A-Za-z0-9]+)(\\(\\s*(?<precision>\\d+)\\s*(,\\s*(?<scale>\\d+)\\s*)?\\))?");

    private static final Pattern ALL_WORD_PATTERN = Pattern.compile("(?<type>[_A-Za-z0-9\\s]+)");
    private static final Pattern ALL_WORD_SPECIAL_PATTERN =
            Pattern.compile("(?<type>[_A-Za-z0-9]+)(\\(\\s*(?<specialStr>[_A-Za-z]+)\\))?");

    public static TypeConfig getTypeConf(String typeStr) {
        typeStr = removeUselessSpace(typeStr);
        TypeConfig typeConfig;
        typeConfig = getAllWordTypeConf(typeStr);
        if (typeConfig != null) {
            return typeConfig;
        }
        typeConfig = getNormalTypeConf(typeStr);
        if (typeConfig != null) {
            return typeConfig;
        }
        typeConfig = getComplexTypeConf(typeStr);
        if (typeConfig != null) {
            return typeConfig;
        }
        typeConfig = getComplex2TypeConf(typeStr);
        if (typeConfig != null) {
            return typeConfig;
        }
        throw new ChunJunRuntimeException("typeStr is not support: " + typeStr);
    }

    private static String removeUselessSpace(String typeStr) {
        typeStr = typeStr.toUpperCase(Locale.ENGLISH).trim();
        if (typeStr.endsWith("NOT NULL")) {
            typeStr = typeStr.substring(0, typeStr.length() - 8);
        }
        typeStr = typeStr.trim();
        // todo Handle type information in the '<>'
        int lessThanIndex = typeStr.indexOf(ConstantValue.LESS_THAN_SIGN);
        if (lessThanIndex != -1 && typeStr.contains(ConstantValue.GREATER_THAN_SIGN)) {
            typeStr = typeStr.substring(0, lessThanIndex);
        }
        return typeStr;
    }

    private static TypeConfig getComplexTypeConf(String typeStr) {
        Matcher matcher = COMPLEX_PATTERN.matcher(typeStr);
        if (matcher.matches()) {
            String prefixType = matcher.group("prefixType");
            String suffixType = matcher.group("suffixType");
            String precision = matcher.group("precision");
            String scale = matcher.group("scale");
            return new TypeConfig(
                    prefixType,
                    suffixType,
                    StringUtils.isEmpty(precision) ? null : Integer.parseInt(precision),
                    StringUtils.isEmpty(scale) ? null : Integer.parseInt(scale));
        }
        return null;
    }

    private static TypeConfig getComplex2TypeConf(String typeStr) {
        Matcher matcher = COMPLEX_PATTERN_2.matcher(typeStr);
        if (matcher.matches()) {
            String prefixType = matcher.group("prefixType");
            String suffixType = matcher.group("suffixType");
            String precision = matcher.group("precision");
            String scale = matcher.group("scale");
            return new TypeConfig(
                    prefixType,
                    suffixType,
                    StringUtils.isEmpty(precision) ? null : Integer.parseInt(precision),
                    StringUtils.isEmpty(scale) ? null : Integer.parseInt(scale));
        }
        return null;
    }

    private static TypeConfig getNormalTypeConf(String typeStr) {
        Matcher matcher = NORMAL_PATTERN.matcher(typeStr);
        if (matcher.matches()) {
            String type = matcher.group("type");
            String precision = matcher.group("precision");
            String scale = matcher.group("scale");
            return new TypeConfig(
                    type,
                    StringUtils.isEmpty(precision) ? null : Integer.parseInt(precision),
                    StringUtils.isEmpty(scale) ? null : Integer.parseInt(scale));
        }
        return null;
    }

    private static TypeConfig getAllWordTypeConf(String typeStr) {
        Matcher matcher = ALL_WORD_PATTERN.matcher(typeStr);
        String type = null;
        if (matcher.matches()) {
            type = matcher.group("type");
        }
        matcher = ALL_WORD_SPECIAL_PATTERN.matcher(typeStr);
        if (matcher.matches()) {
            type = matcher.group("type");
            String specialStr = matcher.group("specialStr");
            if (specialStr != null) {
                type = type + "(" + specialStr + ")";
            }
        }
        if (type != null) {
            return new TypeConfig(type, null, null);
        }
        return null;
    }
}

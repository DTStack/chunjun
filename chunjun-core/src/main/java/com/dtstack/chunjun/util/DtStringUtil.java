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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DtStringUtil {

    private static final Pattern NO_VERSION_PATTERN = Pattern.compile("([a-zA-Z]+).*");

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Split the specified string delimiter --- ignored quotes delimiter
     *
     * @param str
     * @param delimiter
     * @return
     */
    public static List<String> splitIgnoreQuota(String str, char delimiter) {
        List<String> tokensList = new ArrayList<>();
        boolean inQuotes = false;
        boolean inSingleQuotes = false;
        int bracketLeftNum = 0;
        StringBuilder b = new StringBuilder();
        char[] chars = str.toCharArray();
        int idx = 0;
        for (char c : chars) {
            char flag = 0;
            if (idx > 0) {
                flag = chars[idx - 1];
            }
            if (c == delimiter) {
                if (inQuotes) {
                    b.append(c);
                } else if (inSingleQuotes) {
                    b.append(c);
                } else if (bracketLeftNum > 0) {
                    b.append(c);
                } else {
                    tokensList.add(b.toString());
                    b = new StringBuilder();
                }
            } else if (c == '\"' && '\\' != flag && !inSingleQuotes) {
                inQuotes = !inQuotes;
                b.append(c);
            } else if (c == '\'' && '\\' != flag && !inQuotes) {
                inSingleQuotes = !inSingleQuotes;
                b.append(c);
            } else if (c == '(' && !inSingleQuotes && !inQuotes) {
                bracketLeftNum++;
                b.append(c);
            } else if (c == ')' && !inSingleQuotes && !inQuotes) {
                bracketLeftNum--;
                b.append(c);
            } else {
                b.append(c);
            }
            idx++;
        }

        tokensList.add(b.toString());

        return tokensList;
    }

    /**
     * add line Number after new line
     *
     * @param str
     * @return
     */
    public static String addLineNumber(String str) {
        String[] lines = StringUtils.splitByWholeSeparatorPreserveAllTokens(str, "\n");
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= lines.length; i++) {
            int length = String.valueOf(i).length();
            switch (length) {
                case 1:
                    sb.append(i).append(">    ");
                    break;
                case 2:
                    sb.append(i).append(">   ");
                    break;
                case 3:
                    sb.append(i).append(">  ");
                    break;
                default:
                    sb.append(i).append("> ");
                    break;
            }
            sb.append(lines[i - 1]).append("\n");
        }
        return sb.toString();
    }

    public static List<String> splitField(String str) {
        final char delimiter = ',';
        List<String> tokensList = new ArrayList<>();
        boolean inQuotes = false;
        boolean inSingleQuotes = false;
        int bracketLeftNum = 0;
        StringBuilder b = new StringBuilder();
        char[] chars = str.toCharArray();
        int idx = 0;
        for (char c : chars) {
            char flag = 0;
            if (idx > 0) {
                flag = chars[idx - 1];
            }
            if (c == delimiter) {
                if (inQuotes) {
                    b.append(c);
                } else if (inSingleQuotes) {
                    b.append(c);
                } else if (bracketLeftNum > 0) {
                    b.append(c);
                } else {
                    tokensList.add(b.toString());
                    b = new StringBuilder();
                }
            } else if (c == '\"' && '\\' != flag && !inSingleQuotes) {
                inQuotes = !inQuotes;
                b.append(c);
            } else if (c == '\'' && '\\' != flag && !inQuotes) {
                inSingleQuotes = !inSingleQuotes;
                b.append(c);
            } else if (c == '(' && !inSingleQuotes && !inQuotes) {
                bracketLeftNum++;
                b.append(c);
            } else if (c == ')' && !inSingleQuotes && !inQuotes) {
                bracketLeftNum--;
                b.append(c);
            } else if (c == '<' && !inSingleQuotes && !inQuotes) {
                bracketLeftNum++;
                b.append(c);
            } else if (c == '>' && !inSingleQuotes && !inQuotes) {
                bracketLeftNum--;
                b.append(c);
            } else {
                b.append(c);
            }
            idx++;
        }

        tokensList.add(b.toString());

        return tokensList;
    }

    public static String replaceIgnoreQuota(String str, String oriStr, String replaceStr) {
        String splitPatternStr =
                oriStr + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)(?=(?:[^']*'[^']*')*[^']*$)";
        return str.replaceAll(splitPatternStr, replaceStr);
    }

    /**
     * 处理 sql 中 "--" 注释，而不删除引号内的内容
     *
     * @param sql 解析出来的 sql
     * @return 返回无注释内容的 sql
     */
    public static String dealSqlComment(String sql) {
        boolean inQuotes = false;
        boolean inSingleQuotes = false;
        StringBuilder b = new StringBuilder(sql.length());
        char[] chars = sql.toCharArray();
        for (int index = 0; index < chars.length; index++) {
            StringBuilder tempSb = new StringBuilder(2);
            if (index >= 1) {
                tempSb.append(chars[index - 1]);
                tempSb.append(chars[index]);
            }

            if ("--".equals(tempSb.toString())) {
                if (inQuotes) {
                    b.append(chars[index]);
                } else if (inSingleQuotes) {
                    b.append(chars[index]);
                } else {
                    b.deleteCharAt(b.length() - 1);
                    while (chars[index] != '\n') {
                        // 判断注释内容是不是行尾或者 sql 的最后一行
                        if (index == chars.length - 1) {
                            break;
                        }
                        index++;
                    }
                }
            } else if (chars[index] == '\"' && '\\' != chars[index] && !inSingleQuotes) {
                inQuotes = !inQuotes;
                b.append(chars[index]);
            } else if (chars[index] == '\'' && '\\' != chars[index] && !inQuotes) {
                inSingleQuotes = !inSingleQuotes;
                b.append(chars[index]);
            } else {
                b.append(chars[index]);
            }
        }
        return b.toString();
    }

    public static String getPluginTypeWithoutVersion(String engineType) {
        Matcher matcher = NO_VERSION_PATTERN.matcher(engineType);

        if (!engineType.equals("kafka")) {
            return engineType;
        } else if (!matcher.find()) {
            return engineType;
        }

        return matcher.group(1);
    }

    /**
     * add specify params to dbUrl
     *
     * @param dbUrl
     * @param addParams
     * @param isForce true:replace exists param
     * @return
     */
    public static String addJdbcParam(
            String dbUrl, Map<String, String> addParams, boolean isForce) {

        if (Strings.isNullOrEmpty(dbUrl)) {
            throw new RuntimeException("dburl can't be empty string, please check it.");
        }

        if (addParams == null || addParams.size() == 0) {
            return dbUrl;
        }

        String[] splits = dbUrl.split("\\?");
        String preStr = splits[0];
        Map<String, String> params = Maps.newHashMap();
        if (splits.length > 1) {
            String existsParamStr = splits[1];
            String[] existsParams = StringUtils.split(existsParamStr, "&");
            for (String oneParam : existsParams) {
                String[] kv = StringUtils.split(oneParam, "=");
                if (kv.length != 2) {
                    throw new RuntimeException("illegal dbUrl:" + dbUrl);
                }

                params.put(kv[0], kv[1]);
            }
        }

        for (Map.Entry<String, String> addParam : addParams.entrySet()) {
            if (!isForce && params.containsKey(addParam.getKey())) {
                continue;
            }

            params.put(addParam.getKey(), addParam.getValue());
        }

        // rebuild dbURL
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for (Map.Entry<String, String> param : params.entrySet()) {
            if (!isFirst) {
                sb.append("&");
            }

            sb.append(param.getKey()).append("=").append(param.getValue());
            isFirst = false;
        }

        return preStr + "?" + sb;
    }

    public static boolean isJson(String str) {
        boolean flag = false;
        if (StringUtils.isNotBlank(str)) {
            try {
                objectMapper.readValue(str, Map.class);
                flag = true;
            } catch (Throwable e) {
                flag = false;
            }
        }
        return flag;
    }

    public static Object parse(String str, Class clazz) {
        String fieldType = clazz.getName();
        Object object = null;
        if (fieldType.equals(Integer.class.getName())) {
            object = Integer.parseInt(str);
        } else if (fieldType.equals(Long.class.getName())) {
            object = Long.parseLong(str);
        } else if (fieldType.equals(Byte.class.getName())) {
            object = str.getBytes()[0];
        } else if (fieldType.equals(String.class.getName())) {
            object = str;
        } else if (fieldType.equals(Float.class.getName())) {
            object = Float.parseFloat(str);
        } else if (fieldType.equals(Double.class.getName())) {
            object = Double.parseDouble(str);
        } else if (fieldType.equals(Timestamp.class.getName())) {
            object = Timestamp.valueOf(str);
        } else {
            throw new RuntimeException(
                    "no support field type for sql. the input type:" + fieldType);
        }
        return object;
    }

    public static String getTableFullPath(String schema, String tableName) {
        String[] tableInfoSplit = StringUtils.split(tableName, ".");
        // 表明表信息带了schema
        if (tableInfoSplit.length == 2) {
            schema = tableInfoSplit[0];
            tableName = tableInfoSplit[1];
        }

        // 清理首个字符" 和最后字符 "
        schema = rmStrQuote(schema);
        tableName = rmStrQuote(tableName);

        if (StringUtils.isEmpty(schema)) {
            return addQuoteForStr(tableName);
        }

        return addQuoteForStr(schema) + "." + addQuoteForStr(tableName);
    }

    /** 清理首个字符" 和最后字符 " */
    public static String rmStrQuote(String str) {
        if (StringUtils.isEmpty(str)) {
            return str;
        }

        if (str.startsWith("\"")) {
            str = str.substring(1);
        }

        if (str.endsWith("\"")) {
            str = str.substring(0, str.length() - 1);
        }

        return str;
    }

    public static String addQuoteForStr(String column) {
        return getStartQuote() + column + getEndQuote();
    }

    public static String getStartQuote() {
        return "\"";
    }

    public static String getEndQuote() {
        return "\"";
    }

    public static String removeStartAndEndQuota(String str) {
        String removeStart = StringUtils.removeStart(str, "'");
        return StringUtils.removeEnd(removeStart, "'");
    }

    /**
     * 判断当前对象是null 还是空格
     *
     * @param obj 需要判断的对象
     * @return 返回true 如果对象是空格或者为null
     */
    public static boolean isEmptyOrNull(Object obj) {
        return Objects.isNull(obj) || obj.toString().isEmpty();
    }

    /**
     * Capitalize the first letter. The ascii encoding of the letters is moved forward, and the
     * efficiency is higher than the operation of intercepting the string for conversion
     *
     * @param str the string which the first letter should be capitalized.
     * @return the string which the first letter has been capitalized.
     */
    public static String captureFirstLetter(String str) {
        char[] cs = str.toCharArray();
        if (cs[0] > 32) {
            cs[0] -= 32;
        }
        return String.valueOf(cs);
    }
}

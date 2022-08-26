package com.dtstack.chunjun.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author liuwenjie
 * @date 2022/8/26 14:39
 */
public class UnicodeToStringTest {

    public static final Pattern pattern = Pattern.compile("(\\\\u(\\w{4}))");

    @Test
    public void testUnicodeToString() {
        String str = "UNISTR('\\5927\\6D77')";

        if (str.startsWith("UNISTR('") && str.endsWith("')")) {
            String substring = str.substring(8, str.length() - 2);
            String replace = substring.replace("\\", "\\u");
            str = unicodeToString(replace);
        }
        Assert.assertEquals(str, "大海");
    }

    public static String unicodeToString(String str) {
        Matcher matcher = pattern.matcher(str);
        char ch;
        while (matcher.find()) {
            ch = (char) Integer.parseInt(matcher.group(2), 16);
            str = str.replace(matcher.group(1), String.valueOf(ch));
        }
        return str;
    }
}

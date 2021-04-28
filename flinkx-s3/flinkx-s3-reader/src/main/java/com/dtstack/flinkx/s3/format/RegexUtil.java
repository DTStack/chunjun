package com.dtstack.flinkx.s3.format;

import dk.brics.automaton.Automaton;
import dk.brics.automaton.RegExp;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class RegexUtil {

    private static final Map<String, String> PREDEFINED_CHARACTER_CLASSES;

    private static Automaton automaton;

    private static boolean initialized = false;

    static {
        Map<String, String> characterClasses = new HashMap<String, String>();
        characterClasses.put("\\\\d", "[0-9]");
        characterClasses.put("\\\\D", "[^0-9]");
        characterClasses.put("\\\\s", "[ \t\n\f\r]");
        characterClasses.put("\\\\S", "[^ \t\n\f\r]");
        characterClasses.put("\\\\w", "[a-zA-Z_0-9]");
        characterClasses.put("\\\\W", "[^a-zA-Z_0-9]");
        PREDEFINED_CHARACTER_CLASSES = Collections.unmodifiableMap(characterClasses);
    }

    /**
     * 重新设置一个正则表达式
     *
     * @param regex 正则表达式
     */
    public static void initializeOrReset(String regex) {
        RegExp exp = createRegExp(regex);
        automaton = exp.toAutomaton();
        initialized = true;
    }


    private static RegExp createRegExp(String regex) {
        String finalRegex = regex;
        for (Map.Entry<String, String> charClass : PREDEFINED_CHARACTER_CLASSES.entrySet()) {
            finalRegex = finalRegex.replaceAll(charClass.getKey(), charClass.getValue());
        }
        return new RegExp(finalRegex);
    }


    /**
     * 获取当前正则表达式匹配的公共前缀
     *
     * @return 公共前缀
     */
    public static String getCommonPrefix() {
        if (initialized) {
            return automaton.getCommonPrefix();
        } else {
            throw new UnsupportedOperationException("unSupport to getCommonPrefix before initialize");
        }
    }

    /**
     * 匹配一个字符串
     *
     * @param input 待匹配的字符串
     * @return 匹配结果
     */
    public static boolean match(String input) {
        if (initialized) {
            return automaton.run(input);
        } else {
            throw new UnsupportedOperationException("unSupport to match before initialize");
        }

    }

}

package com.macrosan.utils.regex;

import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap;

import java.util.regex.Matcher;

import static com.macrosan.constants.ServerConstants.PROC_NUM;

/**
 * Pattern
 * <p>
 * 带匹配器缓存的Pattern
 *
 * @author liyixin
 * @date 2018/12/29
 */
public class Pattern {

    private final java.util.regex.Pattern internalPattern;

    private ConcurrentHashMap<Integer, Matcher> matchers = new ConcurrentHashMap<>((int) (PROC_NUM * 3 / 0.75) + 1);

    private Pattern(String regex) {
        internalPattern = java.util.regex.Pattern.compile(regex);
    }

    private Pattern(String regex, int flag) {
        internalPattern = java.util.regex.Pattern.compile(regex, flag);
    }

    public static Pattern compile(String regex) {
        return new Pattern(regex);
    }

    public static Pattern compile(String regex, int flag) {
        return new Pattern(regex, flag);
    }

    public Matcher matcher(CharSequence input) {
        Matcher m = matchers.getIfAbsentPut((int) Thread.currentThread().getId(), internalPattern.matcher(input));
        return m.reset(input);
    }
}

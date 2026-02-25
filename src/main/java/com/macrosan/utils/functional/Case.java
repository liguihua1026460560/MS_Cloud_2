package com.macrosan.utils.functional;

import java.util.function.Supplier;

/**
 * Case
 * 提供对控制结构的抽象
 *
 * @author liyixin
 * @date 2018/11/17
 */
public class Case<T> extends ImmutableTuple<Supplier<Boolean>, Supplier<Result<T>>> {

    private Case(Supplier<Boolean> booleanSupplier,
                 Supplier<Result<T>> resultSupplier) {
        super(booleanSupplier, resultSupplier);
    }

    /**
     * 抽象对条件的匹配
     *
     * @param condition 要匹配的条件
     * @param value     匹配成功要执行的操作
     * @return 一次匹配操作
     */
    public static <T> Case<T> mcase(Supplier<Boolean> condition,
                                    Supplier<Result<T>> value) {
        return new Case<>(condition, value);
    }

    /**
     * 抽象对条件的匹配，匹配的默认情况
     *
     * @param value 匹配成功要执行的操作
     * @return 一次匹配操作
     */
    public static <T> DefaultCase<T> mcase(Supplier<Result<T>> value) {
        return new DefaultCase<>(() -> true, value);
    }

    /**
     * 抽象整个匹配逻辑
     *
     * @param defaultCase 默认情况的匹配
     * @param matchers    指定条件的匹配
     * @return 整个匹配结束后返回的操作
     */
    @SafeVarargs
    public static <T> Result<T> match(DefaultCase<T> defaultCase, Case<T>... matchers) {
        for (Case<T> tCase : matchers) {
            if (tCase.var1.get()) {
                return tCase.var2.get();
            }
        }
        return defaultCase.var2.get();
    }

    private static class DefaultCase<T> extends Case<T> {
        private DefaultCase(Supplier<Boolean> booleanSupplier,
                            Supplier<Result<T>> resultSupplier) {
            super(booleanSupplier, resultSupplier);
        }
    }
}

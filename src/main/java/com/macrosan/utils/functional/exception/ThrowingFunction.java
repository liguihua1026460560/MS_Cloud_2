package com.macrosan.utils.functional.exception;

import java.util.function.Function;

/**
 * ThrowingFunction
 *
 * @author liyixin
 * @date 2018/11/24
 */
@FunctionalInterface
public interface ThrowingFunction<T, R, E extends Exception> {
    /**
     * 接受一个参数，返回一个结果
     *
     * @param t 参数
     * @return R 返回值
     * @throws E 方法中的checked异常
     */
    R apply(T t) throws E;

    static <T, R> Function<T, R> throwingFunctionWrapper(ThrowingFunction<T, R, Exception> throwingFunction) {
        return i -> {
            try {
                return throwingFunction.apply(i);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        };
    }
}

package com.macrosan.utils.functional.exception;

import java.util.function.Consumer;

/**
 * ThrowingConsumer
 *
 * @author liyixin
 * @date 2018/11/24
 */
@FunctionalInterface
public interface ThrowingConsumer<T, E extends Exception> {
    /**
     * 接受一个参数，没有返回值
     *
     * @return T 参数
     * @throws E 方法中的checked异常
     */
    void accept(T t) throws E;

    static <T> Consumer<T> throwingConsumerWrapper(ThrowingConsumer<T, Exception> throwingConsumer) {
        return i -> {
            try {
                throwingConsumer.accept(i);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        };
    }
}

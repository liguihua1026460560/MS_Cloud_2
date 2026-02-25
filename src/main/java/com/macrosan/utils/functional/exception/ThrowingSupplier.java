package com.macrosan.utils.functional.exception;

import java.util.function.Supplier;

/**
 * ThrowingSupplier
 *
 * @author liyixin
 * @date 2018/11/24
 */
public interface ThrowingSupplier<T, E extends Exception> {
    /**
     * 不接受参数，返回一个结果
     *
     * @return T 返回值
     * @throws E 方法中的checked异常
     */
    T supply() throws E;

    /**
     * @param throwingSupplier 需要包装的supplier
     * @param <T>              参数
     * @return 包装之后的supplier
     */
    static <T> Supplier<T> throwingSupplierWrapper(ThrowingSupplier<T, Exception> throwingSupplier) {
        return () -> {
            try {
                return throwingSupplier.supply();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        };
    }

}

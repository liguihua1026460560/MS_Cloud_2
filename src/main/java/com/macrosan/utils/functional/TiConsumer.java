package com.macrosan.utils.functional;

/**
 * 接受三个参数的函数式接口
 *
 * @param <T> 第一个参数的参数类型
 * @param <R> 第二个参数的参数类型
 * @param <E> 第三个参数的参数类型
 * @author liyixin
 */
@FunctionalInterface
public interface TiConsumer<T, R, E> {

    /**
     * 应用参数
     *
     * @param t 第一个参数
     * @param r 第二个参数
     * @param e 第三个参数
     */
    void apply(T t, R r, E e);
}

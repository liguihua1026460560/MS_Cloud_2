package com.macrosan.utils.functional;

import io.reactivex.annotations.NonNull;

/**
 * 传入四个参数，返回一个值的函数式接口
 *
 * @author gaozhiyuan
 */
public interface Function4<T1, T2, T3, T4, R> {

    /**
     * @param t1 第一个参数
     * @param t2 第二个参数
     * @param t3 第三个参数
     * @param t4 第四个参数
     * @return 返回值
     */
    @NonNull
    R apply(@NonNull T1 t1, @NonNull T2 t2, @NonNull T3 t3, T4 t4);
}

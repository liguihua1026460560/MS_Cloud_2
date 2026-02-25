package com.macrosan.utils.functional;

import io.reactivex.annotations.NonNull;

/**
 * Function3
 * <p>
 * 传入三个参数，返回一个值的函数式接口
 *
 * @author liyixin
 * @date 2019/5/21
 */
public interface Function3<T1, T2, T3, R> {

    /**
     * @param t1 第一个参数
     * @param t2 第二个参数
     * @param t3 第三个参数
     * @return 返回值
     */
    @NonNull
    R apply(@NonNull T1 t1, @NonNull T2 t2, @NonNull T3 t3);
}

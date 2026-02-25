package com.macrosan.utils.functional;

import io.reactivex.annotations.NonNull;

/**
 * Function2
 * <p>
 * 传入连个参数，返回一个值的函数式接口
 *
 * @author liyixin
 * @date 2019/5/21
 */
public interface Function2<T1, T2, R> {

    /**
     * @param t1 第一个参数
     * @param t2 第二个参数
     * @return 返回值
     */
    @NonNull
    R apply(@NonNull T1 t1, @NonNull T2 t2);
}

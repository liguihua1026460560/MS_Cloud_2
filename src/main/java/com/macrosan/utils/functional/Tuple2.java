package com.macrosan.utils.functional;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * Tuple2
 * <p>
 * 提供可变元组结构，以便后续编写函数式类库使用
 *
 * @author liyixin
 * @date 2018/11/26
 */
@Data
@Accessors(fluent = true)
public class Tuple2<T, U> {

    public T var1;
    public U var2;

    public Tuple2(T var1, U var2) {
        this.var1 = var1;
        this.var2 = var2;
    }

    public Tuple2(T var1) {
        this.var1 = var1;
    }

    public Tuple2() {
    }
}

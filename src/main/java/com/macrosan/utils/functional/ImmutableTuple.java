package com.macrosan.utils.functional;

/**
 * ImmutableTuple
 * 提供不可变元组结构，以便后续编写函数式类库使用
 *
 * @author liyixin
 * @date 2018/11/11
 */
public class ImmutableTuple<T, U> {

    public final T var1;
    public final U var2;

    public ImmutableTuple(T var1, U var2) {
        this.var1 = var1;
        this.var2 = var2;
    }

    @Override
    public String toString() {
        return "ImmutableTuple{" +
                "var1=" + var1 +
                ", var2=" + var2 +
                '}';
    }
}

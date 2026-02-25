package com.macrosan.utils.functional;

/**
 * LongLongTuple
 *
 * @author liyixin
 * @date 2019/1/10
 */
public class LongLongTuple {

    public final long var1;
    public final long var2;

    public LongLongTuple(long var1, long var2) {
        this.var1 = var1;
        this.var2 = var2;
    }

    @Override
    public String toString() {
        return "LongLongTuple{" +
                "var1=" + var1 +
                ", var2=" + var2 +
                '}';
    }
}

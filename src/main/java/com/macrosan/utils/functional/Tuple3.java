package com.macrosan.utils.functional;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

/**
 * Tuple3
 *
 * @author liyixin
 * @date 2019/1/21
 */
@Data
@Accessors(chain = true)
@EqualsAndHashCode
public class Tuple3<T, R, U> {
    public T var1;
    public R var2;
    public U var3;

    public Tuple3(T var1) {
        this.var1 = var1;
    }

    public Tuple3(T var1, R var2) {
        this.var1 = var1;
        this.var2 = var2;
    }

    public Tuple3(T var1, R var2, U var3) {
        this.var1 = var1;
        this.var2 = var2;
        this.var3 = var3;
    }

    public Tuple3() {

    }
}

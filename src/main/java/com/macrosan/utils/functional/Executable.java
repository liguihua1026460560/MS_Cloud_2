package com.macrosan.utils.functional;

/**
 * Executable
 * 函数式接口，代表一个可以执行的无返回值的动作
 *
 * @author liyixin
 * @date 2018/11/12
 */
@FunctionalInterface
public interface Executable {
    /**
     * 执行动作
     */
    void exec();
}

package com.macrosan.database.redis;

import io.lettuce.core.RedisFuture;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Command
 * 命令的实体类
 *
 * @author liyixin
 * @date 2018/12/1
 */
public class Command<R> {

    /**
     * 一个参数的命令
     */
    private Function<String, RedisFuture<R>> siCommand;

    /**
     * 两个参数的命令
     */
    private BiFunction<String, String, RedisFuture<R>> biCommand;

    private final String param1;

    private final String param2;

    public Command(Function<String, RedisFuture<R>> siCommand, String param1) {
        this.siCommand = siCommand;
        this.param1 = param1;
        this.param2 = "";
    }

    public Command(BiFunction<String, String, RedisFuture<R>> biCommand, String param1, String param2) {
        this.biCommand = biCommand;
        this.param1 = param1;
        this.param2 = param2;
    }

    public RedisFuture<R> apply() {
        return siCommand == null ? biCommand.apply(param1, param2) : siCommand.apply(param1);
    }
}

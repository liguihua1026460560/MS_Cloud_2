package com.macrosan.utils.listutil;

import reactor.core.publisher.Flux;

import java.util.function.Function;

public interface ListOperation<T> extends Function<String, Flux<T>> {
    @Override
    Flux<T> apply(String s);
}

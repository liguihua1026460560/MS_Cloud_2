package com.macrosan.storage.metaserver.move.scanner;

import reactor.core.publisher.Flux;

public interface Scanner<T> {

    Flux<T> scan();

    void tryEnd();

    boolean isEnd();

    boolean hasRemaining();
}

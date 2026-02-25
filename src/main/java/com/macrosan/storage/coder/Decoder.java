package com.macrosan.storage.coder;

import reactor.core.publisher.Flux;

/**
 * @author gaozhiyuan
 */
public interface Decoder {
    /**
     * 解码后的数据流
     *
     * @return 解码后的数据流
     */
    Flux<byte[]> res();
}

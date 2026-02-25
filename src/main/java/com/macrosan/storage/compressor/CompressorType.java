package com.macrosan.storage.compressor;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * @description: 压缩算法类型
 * @author: wanhao
 * @date: 2022-08-29 11:07:50
 **/
@Retention(RetentionPolicy.RUNTIME)
public @interface CompressorType {
    String value();
}

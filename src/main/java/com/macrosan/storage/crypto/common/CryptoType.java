package com.macrosan.storage.crypto.common;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * @Description: 应用于加密算法的实现类上，用于指定加密实现类的算法类型
 * @Author wanhao
 * @Date 2023/1/9 上午 10:09
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface CryptoType {
    CryptoAlgorithm value();
}

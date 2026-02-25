package com.macrosan.storage.compressor;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @description: 压缩算法工厂，用于启动时加载所有配置的压缩算法、根据算法名获取对应的压缩算法
 * @author: wanhao
 * @date: 2022-08-29 10:59:20
 **/
public class CompressorFactory {
    private static final Map<String, Compressor> COMPRESSOR_MAP = new ConcurrentHashMap<>();

    private CompressorFactory() {
    }

    public static void init() {
        ServiceLoader<Compressor> service = ServiceLoader.load(Compressor.class);
        for (Compressor compress : service) {
            CompressorType compressType = compress.getClass().getAnnotation(CompressorType.class);
            if (compressType != null) {
                String compressName = compressType.value();
                COMPRESSOR_MAP.put(compressName, compress);
            }
        }
    }

    public static Compressor getCompressor(String compressorName) {
        return COMPRESSOR_MAP.get(compressorName);
    }
}

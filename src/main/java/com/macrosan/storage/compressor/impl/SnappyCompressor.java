package com.macrosan.storage.compressor.impl;

import com.macrosan.storage.compressor.Compressor;
import com.macrosan.storage.compressor.CompressorType;
import org.xerial.snappy.Snappy;

import java.io.IOException;

/**
 * @author wanhao
 * @description: snappy压缩算法
 * @date 2022/8/29 0029上午 11:03
 */
@CompressorType("snappy")
public class SnappyCompressor implements Compressor {
    @Override
    public byte[] compress(byte[] data) throws IOException {
        return Snappy.compress(data);
    }

    @Override
    public byte[] uncompress(byte[] data) throws IOException {
        return Snappy.uncompress(data);
    }
}

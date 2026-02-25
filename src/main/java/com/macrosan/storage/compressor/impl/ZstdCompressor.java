package com.macrosan.storage.compressor.impl;

import com.github.luben.zstd.NoPool;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import com.macrosan.fs.BlockDevice;
import com.macrosan.storage.compressor.Compressor;
import com.macrosan.storage.compressor.CompressorType;
import lombok.extern.log4j.Log4j2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author wanhao
 * @description: zstd压缩算法
 * @date 2022/8/29 0029下午 5:39
 */
@Log4j2
@CompressorType("zstd")
public class ZstdCompressor implements Compressor {
    @Override
    public byte[] compress(byte[] data) throws IOException {
        return Zstd.compress(data);
    }

    @Override
    public byte[] uncompress(byte[] data) {
        byte[] output;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length);
             ZstdInputStream zis = new ZstdInputStream(new ByteArrayInputStream(data), NoPool.INSTANCE)) {
            byte[] buf = new byte[BlockDevice.BLOCK_SIZE];
            int len;
            while ((len = zis.read(buf)) != -1) {
                bos.write(buf, 0, len);
            }
            output = bos.toByteArray();
        } catch (Exception e) {
            log.error("zstd uncompress: ", e);
            output = data;
        }
        return output;
    }

}

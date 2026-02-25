package com.macrosan.storage.compressor.impl;

import com.macrosan.fs.BlockDevice;
import com.macrosan.storage.compressor.Compressor;
import com.macrosan.storage.compressor.CompressorType;
import lombok.extern.log4j.Log4j2;
import net.jpountz.lz4.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author wanhao
 * @description: lz4压缩算法
 * @date 2022/8/29 下午 6:38
 */
@Log4j2
@CompressorType("lz4")
public class Lz4Compressor implements Compressor {
    LZ4Factory factory;

    @Override
    public byte[] compress(byte[] data) throws IOException {
        byte[] output;
        LZ4Compressor compressor = factory.fastCompressor();
        try (ByteArrayOutputStream byteOutput = new ByteArrayOutputStream(data.length);
             LZ4BlockOutputStream compressedOutput = new LZ4BlockOutputStream(byteOutput, BlockDevice.BLOCK_SIZE, compressor)) {
            compressedOutput.write(data);
            output = byteOutput.toByteArray();
        } catch (Exception e) {
            log.error("lz4 compress: ", e);
            output = data;
        }
        return output;
    }

    @Override
    public byte[] uncompress(byte[] data) {
        byte[] output;
        LZ4FastDecompressor decompresser = factory.fastDecompressor();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length);
             LZ4BlockInputStream lzis = new LZ4BlockInputStream(new ByteArrayInputStream(data), decompresser)) {
            int count;
            byte[] buffer = new byte[BlockDevice.BLOCK_SIZE];
            while ((count = lzis.read(buffer)) != -1) {
                baos.write(buffer, 0, count);
            }
            output = baos.toByteArray();
        } catch (Exception e) {
            log.error("lz4 uncompress: ", e);
            output = data;
        }
        return output;
    }

    public Lz4Compressor() {
        factory = LZ4Factory.fastestInstance();
    }
}
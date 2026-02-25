package com.macrosan.storage.compressor.impl;

import com.macrosan.fs.BlockDevice;
import com.macrosan.storage.compressor.Compressor;
import com.macrosan.storage.compressor.CompressorType;
import lombok.extern.log4j.Log4j2;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * @author wanhao
 * @description: Zlib压缩算法
 * @date 2022/9/1 下午 5:40
 */
@Log4j2
@CompressorType("zlib")
public class ZlibCompressor implements Compressor {
    @Override
    public byte[] compress(byte[] data) throws IOException {
        byte[] output;
        Deflater compresser = new Deflater();

        compresser.reset();
        compresser.setInput(data);
        compresser.finish();
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length)) {
            byte[] buf = new byte[BlockDevice.BLOCK_SIZE];
            while (!compresser.finished()) {
                int i = compresser.deflate(buf);
                bos.write(buf, 0, i);
            }
            output = bos.toByteArray();
        } catch (Exception e) {
            output = data;
            log.error("zlib compress: ", e);
        } finally {
            compresser.end();
        }
        return output;

    }

    @Override
    public byte[] uncompress(byte[] data) {
        byte[] output;

        Inflater decompresser = new Inflater();
        decompresser.reset();
        decompresser.setInput(data);

        try (ByteArrayOutputStream o = new ByteArrayOutputStream(data.length)) {
            byte[] buf = new byte[BlockDevice.BLOCK_SIZE];
            while (!decompresser.finished()) {
                int i = decompresser.inflate(buf);
                o.write(buf, 0, i);
            }
            output = o.toByteArray();
        } catch (Exception e) {
            output = data;
            log.error("zlib uncompress: ", e);
        } finally {
            decompresser.end();
        }

        return output;
    }

}

package com.macrosan.rsocket.filesystem;

import lombok.extern.log4j.Log4j2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * DefaultFileReader
 * <p>
 * 普通文件的读取
 *
 * @author liyixin
 */
@Log4j2
public class DefaultFileReader implements MsFileReader {

    private RandomAccessFile internalReader;

    DefaultFileReader(String sourcePath) throws FileNotFoundException {
        internalReader = new RandomAccessFile(sourcePath, "r");
    }

    @Override
    public int read(byte[] b) throws IOException {
        return internalReader.read(b);
    }

    @Override
    public MsFileReader seek(long pos) throws IOException {
        internalReader.seek(pos);
        return this;
    }

    @Override
    public void close() throws IOException {
        if (internalReader != null) {
            internalReader.close();
        }
    }

    @Override
    public long length() throws IOException {
        return internalReader.length();
    }
}

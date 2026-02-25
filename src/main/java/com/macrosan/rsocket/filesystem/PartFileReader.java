package com.macrosan.rsocket.filesystem;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * PartFileReader
 * <p>
 * 分段文件的读取
 *
 * @author liyixin
 */
public class PartFileReader implements MsFileReader {

    private static final Logger logger = LogManager.getLogger(PartFileReader.class.getSimpleName());

    private RandomAccessFile internalReader;

    private PartFileMapping mapping;

    private static final String REGEX = "\\".equals(File.separator) ? "[\\\\|V]" : "[/|V]";

    PartFileReader(String sourcePath) {
        String[] array = sourcePath.split(REGEX);
        String objId = array[array.length - 1];
        String vnode = array[array.length - 2];
        mapping = new PartFileMapping(objId, vnode);
    }

    /**
     * 注：调用该方法前必须先seek
     *
     * @param b 作为容器的byte数组
     * @return 读取到的长度
     */
    @Override
    public int read(byte[] b) throws IOException {
        int count = 0;
        try {
            while (count < b.length) {
                int n = internalReader.read(b, count, b.length - count);
                if (n < 0) {
                    internalReader.close();
                    String path = mapping.getNextFile();
                    internalReader = new RandomAccessFile(path, "r");
                    logger.debug("locating part file : " + path);
                } else {
                    count = count + n;
                }
            }
        } catch (NullPointerException e) {
            logger.error("This reader has not been sought");
        } catch (FileNotFoundException e) {
            logger.error("The file has reach the end");
        }
        return count;
    }

    @Override
    public MsFileReader seek(long pos) throws IOException {
        long relativePos = mapping.getRelativePos(pos);
        String path = mapping.getCurrentFile();
        if (internalReader != null) {
            internalReader.close();
        }
        internalReader = new RandomAccessFile(path, "r");
        logger.debug("locating part file : " + path);
        internalReader.seek(relativePos);
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

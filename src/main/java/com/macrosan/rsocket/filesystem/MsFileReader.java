package com.macrosan.rsocket.filesystem;

import java.io.IOException;

/**
 * MsFileReader
 * <p>
 * 对普通文件和分段文件的抽象 TODO 加入NIO 和 零拷贝的支持，优化IO效率
 *
 * @author liyixin
 * @date 2019/3/8
 */
public interface MsFileReader extends AutoCloseable {

    /**
     * @param b 作为容器的byte数组
     * @return 读取到的byte 数量
     */
    int read(byte[] b) throws IOException;

    /**
     * @param pos 定位到 pos
     */
    MsFileReader seek(long pos) throws IOException;

    /**
     * 释放资源
     */
    @Override
    void close() throws IOException;

    long length() throws IOException;
}

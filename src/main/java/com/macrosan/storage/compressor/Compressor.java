package com.macrosan.storage.compressor;

import java.io.IOException;

/**
 * @description: 压缩算法接口，算法实现在impl目录下
 * 项目需要生效的压缩算法配置在/META-INF/service/com.macrosan.storage.compressor.Compressor文件中
 * @author: wanhao
 * @date: 2022-08-29 11:01:16
 **/
public interface Compressor {
    /**
     * @param data: 待压缩的数据
     * @return 压缩后的数据
     * @author: wanhao
     * @date: 2022-08-29 11:14:58
     **/
    byte[] compress(byte[] data) throws IOException;

    /**
     * @param data:待解压的数据
     * @return 解压后的数据
     * @author: wanhao
     * @date: 2022-08-29 11:14:34
     **/
    byte[] uncompress(byte[] data) throws IOException;

}

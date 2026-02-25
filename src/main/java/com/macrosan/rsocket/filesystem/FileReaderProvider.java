package com.macrosan.rsocket.filesystem;

import com.macrosan.constants.SysConstants;
import org.apache.commons.lang3.StringUtils;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * FileReaderProvider
 * <p>
 * 工厂模式，提供文件读取类
 *
 * @author liyixin
 */
public class FileReaderProvider {

    public static MsFileReader getReader(String path, String type) throws FileNotFoundException {
        return StringUtils.isBlank(type) ? new DefaultFileReader(path) : new PartFileReader(path);
    }

    public static MsFileReader getAndInitReader(String path, String type, long start) throws IOException {
        return StringUtils.isBlank(type) ?
                new DefaultFileReader(path).seek(start + SysConstants.META_SYS_MAX_SIZE) :
                new PartFileReader(path).seek(start);
    }


    public static MsFileReader getECReader(String path, String type, long start) throws IOException {
        return StringUtils.isBlank(type) ?
                new DefaultFileReader(path).seek(start) :
                new PartFileReader(path).seek(start);
    }
}

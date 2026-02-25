package com.macrosan.utils.msutils;

/**
 * 不记录log的异常，防止日志杂乱或写日志的io负载过重
 *
 * @author fanjunxi
 */
public class EscapeException extends RuntimeException {

    public EscapeException(String message) {
        super(message);
    }
}

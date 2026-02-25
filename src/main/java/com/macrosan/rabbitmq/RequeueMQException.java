package com.macrosan.rabbitmq;

/**
 * @author zhaoyang
 * @date 2024/11/13
 **/
public class RequeueMQException  extends RuntimeException{
    // 不记录堆栈信息
    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }

    public RequeueMQException(String message) {
        super(message);
    }

    public RequeueMQException() {
        super();
    }
}

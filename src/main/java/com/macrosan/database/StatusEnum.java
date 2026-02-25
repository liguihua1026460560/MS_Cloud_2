package com.macrosan.database;

public enum StatusEnum {

    /**
     * 表明一个连接是活跃的
     */
    ALIVE("ALIVE"),
    /**
     * 表明一个连接是空闲的，下一轮检测中会被关闭
     */
    IDLE("idle"),
    /**
     * 表明一个连接已经被关闭了
     */
    CLOSED("closed");

    private String statusName;

    StatusEnum(String statusName) {
        this.statusName = statusName;
    }
}

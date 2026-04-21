package com.macrosan.component.pojo;

import lombok.Data;

/**
 * @author zhaoyang
 * @date 2023/10/26
 **/
@Data
public class MediaResult {
    private int code;

    private String msg;

    private Object data;

    public boolean isHeartbeat() {
        return "heartbeat".equals(msg);
    }

}

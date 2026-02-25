package com.macrosan.message.jsonmsg;

import lombok.Data;

@Data
public class MessageSend {
    String sourceIPAddress;
    String principalId;
    String requestId;
    String configurationId;
    String eventName;
    String time;
    String version;
    String objectName;
    String[] event;
    String bucketName;
    String region;
    String userId;
}

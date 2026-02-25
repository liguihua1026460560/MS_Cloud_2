package com.macrosan.utils.asm;

import com.macrosan.message.jsonmsg.fast.MsObjectMapper;
import io.vertx.core.json.Json;

/**
 * @author gaozhiyuan
 */
public class ASM {
    public static void init() {
        System.setProperty("vertx.disableWebsockets", "false");
        Json.mapper = new MsObjectMapper();
        BaseMpscLinkedArrayQueueRepair.init();
        RSocketRequesterRepair.init();
        HttpServerResponseImplRepair.init();
        ConsoleToLog.init();
        RabbitmqPublish.init();
        OpenSSLMessageDigestNativeRepair.init();
        BindEpoll.init();
    }
}

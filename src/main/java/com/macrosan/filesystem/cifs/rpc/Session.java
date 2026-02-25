package com.macrosan.filesystem.cifs.rpc;

import io.vertx.reactivex.core.net.NetSocket;
import lombok.extern.log4j.Log4j2;

import javax.crypto.Cipher;
import java.util.HashMap;
import java.util.Map;

@Log4j2
public class Session {
    public byte[] exportedSessionKey;
    public String curServerIP;
    public String clientIP;

    public NetSocket socket;

    public Cipher clientCipher;
    public Cipher serverCipher;
    // <v1,v2> v1: clientIp, v2: <severIP, port>
    public static Map<String, Map<String, Integer>> witnessPort = new HashMap<>();

    public boolean socketIsClose = false;
}


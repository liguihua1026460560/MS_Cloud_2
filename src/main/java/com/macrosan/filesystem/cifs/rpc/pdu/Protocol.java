package com.macrosan.filesystem.cifs.rpc.pdu;

import java.util.HashMap;
import java.util.Map;

public class Protocol {

    public byte type;
    public Protocol(byte type) {
        this.type = type;
    }

    public static class IP extends Protocol {
        public byte[] ip = new byte[4];

        public IP(byte type) {
            super(type);
        }

        public String getIP() {
            return (ip[0] & 0xFF) + "." +
                    (ip[1] & 0xFF) + "." +
                    (ip[2] & 0xFF) + "." +
                    (ip[3] & 0xFF);
        }
    }

    public static class RPC extends Protocol{
        public short pad;   // 空内容

        public RPC(byte type) {
            super(type);
        }
    }

    public static class TCP extends Protocol {
        public byte[] port = new byte[2];

        public TCP(byte type) {
            super(type);
        }

        public int getPort() {
            return ((port[0] & 0xFF) << 8) | ((port[1] & 0xFF));
        }
    }

    public static class UUID extends Protocol{
        public byte[] uuid = new byte[16];
        public short version;
        public short versionMinor;
        public UUID(byte type) {
            super(type);
        }
    }
}

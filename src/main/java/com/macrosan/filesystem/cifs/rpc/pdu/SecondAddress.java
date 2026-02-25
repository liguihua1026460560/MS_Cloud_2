package com.macrosan.filesystem.cifs.rpc.pdu;

import lombok.Data;

@Data
public class SecondAddress {
    public short length;
    public byte[] port;

    public SecondAddress() {
        length = 0;
        port = new byte[0];
    }
}

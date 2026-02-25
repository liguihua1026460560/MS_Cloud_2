package com.macrosan.filesystem.cifs.rpc.witness.ntlmssp;

import com.macrosan.filesystem.ReadStruct;
import io.netty.buffer.ByteBuf;
import lombok.extern.log4j.Log4j2;

// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/b34032e5-3aae-4bc6-84c3-c6d80eadf7f2
@Log4j2
public class Negotiate implements ReadStruct {
    public byte[] signature = new byte[8];
    public int messageType;    // 0x01
    public int negotiateFlags;
    public Fields domainNameFields;
    public Fields workstationFields;

    public long version;

    public byte[] domainName;
    public byte[] workstation;


    @Override
    public int readStruct(ByteBuf buf, int offset) {
        buf.getBytes(offset, signature);

        messageType = buf.getIntLE(offset + 8);
        negotiateFlags = buf.getIntLE(offset + 12);

        domainNameFields = new Fields();
        domainNameFields.len = buf.getShortLE(offset + 16);
        domainNameFields.maxLen = buf.getShortLE(offset + 18);
        domainNameFields.bufferOffset = buf.getIntLE(offset + 20);

        workstationFields = new Fields();
        workstationFields.len = buf.getShortLE(offset + 24);
        workstationFields.maxLen = buf.getShortLE(offset + 26);
        workstationFields.bufferOffset = buf.getIntLE(offset + 28);

        version = buf.getLongLE(offset + 32);

        // domainName 和 workstation 数据不存在，所以不做处理
        if (domainNameFields.len > 0) {
            domainName = new byte[domainNameFields.len];
            buf.getBytes(offset + 40, domainName);
        }
        if(workstationFields.len > 0) {
            workstation = new byte[workstationFields.len];
            buf.getBytes(offset + domainNameFields.len, workstation);
        }
        return 40 + domainNameFields.len + workstationFields.len;
    }
}

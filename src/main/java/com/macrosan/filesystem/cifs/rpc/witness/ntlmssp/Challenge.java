package com.macrosan.filesystem.cifs.rpc.witness.ntlmssp;

import com.macrosan.filesystem.cifs.rpc.Session;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

import static com.macrosan.filesystem.cifs.rpc.witness.ntlmssp.Challenge.AvId.*;

@Data
public class Challenge {
    public short size; // 不写入协议
    public byte[] signature = new byte[8];
    public int messageType;  // 0x02
    public Fields targetNameFields;
    public int negotiateFlags;
    public byte[] serverChallenge = new byte[8];  // 随机数
    public byte[] reserved = new byte[8];
    public Fields targetInfoFields;
    public long version;
    public byte[] targetName;
    public Av[] avs;

    public Challenge(Negotiate negotiate, Session session) {
        byte[] domain = "MOSS".getBytes(StandardCharsets.UTF_16LE);
        byte[] cn = "MOSS".getBytes(StandardCharsets.UTF_16LE);
        byte[] dnsCN = "moss.com".getBytes(StandardCharsets.UTF_16LE);

        signature = negotiate.signature;
        messageType = 0x02;
        negotiateFlags = 0xe2898215;
        new SecureRandom().nextBytes(serverChallenge);
//        if (session != null) {
//            session.serverChallenge = serverChallenge;
//        }

        targetName = domain;
        targetNameFields = new Fields((short) domain.length, (short) domain.length, 56);

        avs = new Av[6];
        avs[0] = new Av(MsvAvNbDomainName, domain.length, domain);
        avs[1] = new Av(MsvAvNbComputerName, cn.length, cn);
        avs[2] = new Av(MsvAvDnsDomainName, 0, new byte[0]);
        avs[3] = new Av(MsvAvDnsComputerName, dnsCN.length, dnsCN);
        avs[4] = new Av(MsvAvTimestamp, 8, System.currentTimeMillis());
        avs[5] = new Av(MsvAvEOL, 0, new byte[0]);
        short targetInfoSize = 0;
        for (int i = 0; i < 6; i++) {
            targetInfoSize += 4 + avs[i].len;
        }
        targetInfoFields = new Fields(targetInfoSize, targetInfoSize, 56 + domain.length);

        version = negotiate.version;

        size = (short) (56 + targetNameFields.len + targetInfoFields.len);
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int start = offset;
        buf.setBytes(start, signature);
        buf.setIntLE(start + 8, messageType);
        start += targetNameFields.writeStruct(buf, start + 12) + 12;
        buf.setIntLE(start, negotiateFlags);
        buf.setBytes(start + 4, serverChallenge);
        buf.setBytes(start + 12, reserved);
        start += targetInfoFields.writeStruct(buf, start + 20) + 20;
        buf.setLongLE(start, version);
        buf.setBytes(start + 8, targetName);
        start = start + 8 + targetName.length;
        for (int i = 0; i < avs.length; i++) {
            start += avs[i].writeStruct(buf, start);
        }
        return start - offset;
    }


    @Data
    @NoArgsConstructor
    public class Av {
        public AvId avId;
        public short len;
        public byte[] value;

        public Av(AvId avId, int len, byte[] value) {
            this.avId = avId;
            this.len = (short) len;
            this.value = value;
        }

        public Av(AvId avId, int len, long timestamp) {
            this.avId = avId;
            this.len = (short) len;
            this.value = new byte[8];
            for (int i = 0; i < 8; i++) {
                this.value[7 - i] = (byte) ((timestamp >> (8 * i)) & 0xFF);
            }
        }

        public int writeStruct(ByteBuf buf, int offset) {
            buf.setShortLE(offset, avId.value);
            buf.setShortLE(offset + 2, len);
            buf.setBytes(offset + 4, value);
            return 4 + value.length;
        }
    }

    public enum AvId {
        MsvAvEOL(0x0000),
        MsvAvNbComputerName(0x0001),
        MsvAvNbDomainName(0x0002),
        MsvAvDnsComputerName(0x0003),
        MsvAvDnsDomainName(0x0004),
        MsvAvDnsTreeName(0x0005),
        MsvAvFlags(0x0006),
        MsvAvTimestamp(0x0007),
        MsvAvSingleHost(0x0008),
        MsvAvTargetName(0x0009),
        MsvAvChannelBindings(0x000A);

        private final short value;

        AvId(int v) {
            this.value = (short) v;
        }
    }
}

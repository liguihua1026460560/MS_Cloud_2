package com.macrosan.filesystem.cifs.types;

import com.macrosan.constants.ErrorNo;
import com.macrosan.filesystem.ReadStruct;
import com.macrosan.filesystem.cifs.types.Spnego.SpnegoTokenInitMessage;
import com.macrosan.filesystem.cifs.types.Spnego.SpnegoTokenTargMessage;
import com.macrosan.filesystem.utils.CifsUtils;
import com.macrosan.utils.msutils.MsException;
import io.netty.buffer.ByteBuf;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class NTLMSSP {
    public static NTLMSSPMessage readStruct(ByteBuf buf, int offset) {
        long sign = buf.getLong(offset);
        if (sign != 0x4e544c4d53535000L) {
            switch (buf.getByte(offset)) {
                //negTokenInit
                case 0x60:
                    SpnegoTokenInitMessage initMsg = new SpnegoTokenInitMessage();
                    initMsg.readStruct(buf, offset);
                    return initMsg;
                case (byte) 0xa1:
                    SpnegoTokenTargMessage targMsg = new SpnegoTokenTargMessage();
                    targMsg.readStruct(buf, offset);
                    return targMsg;
            }

            return null;
        }

        int type = buf.getIntLE(offset + 8);
        if (type == 1) {
            NTLMNegotiate msg = new NTLMNegotiate();
            msg.readStruct(buf, offset);
            return msg;
        } else if (type == 2) {
            NTLMChallenge msg = new NTLMChallenge();
            msg.readStruct(buf, offset);
            return msg;
        } else if (type == 3) {
            NTLMAuth msg = new NTLMAuth();
            msg.readStruct(buf, offset);
            return msg;
        }

        return null;
    }

    public static abstract class NTLMSSPMessage implements ReadStruct {
        public int writeStruct(ByteBuf buf, int offset) {
            return 0;
        }

        public int size() {
            return 0;
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/b34032e5-3aae-4bc6-84c3-c6d80eadf7f2
    public static class NTLMNegotiate extends NTLMSSPMessage {
        long sign;
        int type;
        public int flags;
        byte[] domain;
        byte[] workstation;

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            sign = buf.getLong(offset);
            type = buf.getIntLE(offset + 8);
            flags = buf.getIntLE(offset + 12);
            domain = CifsUtils.readBytes(buf, offset, 16);
            workstation = CifsUtils.readBytes(buf, offset, 24);
            return 32 + domain.length + workstation.length;
        }

        @Override
        public String toString() {
            return "NTLMNegotiate{" +
                    "sign=0x" + Long.toHexString(sign) +
                    ", type=" + type +
                    ", flags=" + flags +
                    ", domain=" + new String(domain) +
                    ", workstation=" + new String(workstation) +
                    '}';
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/801a4681-8809-4be9-ab0d-61dcfe762786
    public static class NTLMChallenge extends NTLMSSPMessage {
        long sign = 0x4e544c4d53535000L;
        int type = 2;
        public String targetName;
        public int flags;
        public byte[] serverChallenge;
        public Map<TargetKey, Object> targetInfo = new HashMap<>();
        public long version;

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            buf.setLong(offset, sign);
            buf.setIntLE(offset + 8, type);
            int dataOff = 56;

//            CifsUtils.writeBytes(buf, targetName, offset, 12, dataOff);

            char[] targetChs = targetName.toCharArray();
            buf.setShortLE(offset + 12, targetChs.length * 2);
            buf.setShortLE(offset + 14, targetChs.length * 2);
            buf.setIntLE(offset + 16, dataOff);

            for (int i = 0; i < targetChs.length; i++) {
                buf.setShortLE(dataOff + offset, targetChs[i]);
                dataOff += 2;
            }

            buf.setIntLE(offset + 20, flags);
            buf.setBytes(offset + 24, serverChallenge);


            int targetInfoSize = targetInfoBytes(buf, dataOff + offset);
            buf.setShortLE(offset + 40, targetInfoSize);
            buf.setShortLE(offset + 42, targetInfoSize);
            buf.setIntLE(offset + 44, dataOff);
            buf.setLongLE(offset + 48, version);

            return dataOff + targetInfoSize;
        }

        private int targetInfoBytes(ByteBuf buf, int offset) {
            int size = 0;
            for (TargetKey key : targetInfo.keySet()) {
                Object o = targetInfo.get(key);
                if (o.getClass().equals(String.class)) {
                    char[] chs = ((String) o).toCharArray();
                    buf.setShortLE(offset, key.ordinal());
                    buf.setShortLE(offset + 2, chs.length * 2);
                    offset += 4;
                    for (int i = 0; i < chs.length; i++) {
                        buf.setShortLE(offset, chs[i]);
                        offset += 2;
                    }

                    size += 4 + chs.length * 2;
                } else if (o.getClass().equals(Long.class)) {
                    buf.setShortLE(offset, key.ordinal());
                    buf.setShortLE(offset + 2, 8);
                    buf.setLongLE(offset + 4, (Long) o);
                    offset += 12;
                    size += 12;
                }
            }

            buf.setIntLE(offset, 0);
            return size + 4;
        }

        private int targetInfoSize() {
            int size = 0;
            for (TargetKey key : targetInfo.keySet()) {
                Object o = targetInfo.get(key);
                if (o.getClass().equals(String.class)) {
                    size += 4 + ((String) o).toCharArray().length * 2;
                } else if (o.getClass().equals(Long.class)) {
                    size += 12;
                }
            }

            return size + 4;
        }

        public int size() {
            return 56 + targetInfoSize() + targetName.toCharArray().length * 2;
        }

        @Override
        public int readStruct(ByteBuf buf, int offset) {
//            sign = buf.getLong(offset);
//            type = buf.getIntLE(offset + 8);
//            targetName = CifsUtils.readBytes(buf, offset, 12);
//            flags = buf.getIntLE(offset + 20);
//            serverChallenge = new byte[8];
//            buf.getBytes(offset + 24, serverChallenge);
//
//            int targetInfoSize = buf.getShortLE(offset + 40) & 0xff;
//            if (targetInfoSize > 0) {
//                int targetInfoOff = buf.getIntLE(offset + 44);
//                byte[] targetInfos = new byte[targetInfoSize];
//                buf.getBytes(targetInfoOff, targetInfos);
//
//                int off = targetInfoOff + offset;
//                while (off < targetInfoOff + targetInfoSize + offset) {
//                    TargetKey key = TargetKey.values()[buf.getShortLE(off)];
//                    int len = buf.getShortLE(off + 2) & 0xff;
//                    byte[] target = new byte[len];
//                    buf.getBytes(off + 4, target);
//                    targetInfo.put(key, new String(target));
//                    off += 4 + len;
//                }
//            }
//            version = buf.getLongLE(offset + 48);
//
//            return targetName.length + targetInfoSize + 56;
            return 0;
        }

        @Override
        public String toString() {
            return "NTLMChallenge{" +
                    "sign=0x" + Long.toHexString(sign) +
                    ", type=" + type +
                    ", targetName=" + new String(targetName) +
                    ", flags=" + flags +
                    ", serverChallenge=" + Arrays.toString(serverChallenge) +
                    ", targetInfo=" + targetInfo +
                    ", version=" + version +
                    '}';
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/83f5e789-660d-4781-8491-5f8c6641f75e
    public enum TargetKey {
        EOL,
        NbComputerName,
        NbDomainName,
        DnsComputerName,
        DnsDomainName,
        DnsTreeName,
        Flags,
        Timestamp,
        SingleHost,
        TargetName,
        ChannelBindings
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/033d32cc-88f9-4483-9bf2-b273055038ce
    public static class NTLMAuth extends NTLMSSPMessage {
        long sign;
        int type;
        byte[] lanResponse;
        public byte[] ntlmResponse;
        public byte[] domain;
        public byte[] userName;
        public byte[] hostName;
        public byte[] seesionKey;
        public int flags;


        @Override
        public int readStruct(ByteBuf buf, int offset) {
            sign = buf.getLongLE(offset);
            type = buf.getIntLE(offset + 8);
            lanResponse = CifsUtils.readBytes(buf, offset, 12);
            ntlmResponse = CifsUtils.readBytes(buf, offset, 20);
            domain = CifsUtils.readBytes(buf, offset, 28);
            userName = CifsUtils.readBytes(buf, offset, 36);
            hostName = CifsUtils.readBytes(buf, offset, 44);
            seesionKey = CifsUtils.readBytes(buf, offset, 52);
            flags = buf.getIntLE(offset + 60);
            return 64 + lanResponse.length + ntlmResponse.length + domain.length + userName.length + hostName.length
                    + seesionKey.length;
        }

        @Override
        public String toString() {
            return "NTLMAuth{" +
                    "sign=0x" + Long.toHexString(sign) +
                    ", type=" + type +
                    ", lanResponse=" + Arrays.toString(lanResponse) +
                    ", ntlmResponse=" + Arrays.toString(ntlmResponse) +
                    ", domain=" + new String(domain) +
                    ", userName=" + new String(userName) +
                    ", hostName=" + new String(hostName) +
                    ", seesionKey=" + Arrays.toString(seesionKey) +
                    ", flags=" + flags +
                    '}';
        }

        //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/f9e6fbc4-a953-4f24-b229-ccdcc213b9ec
        public boolean checkAuth(String domain, String account, String passwd, byte[] serverChallenge) {
            if (ntlmResponse.length < 16) {
                return false;
            }

            byte[] response = Arrays.copyOf(ntlmResponse, 16);
            byte[] authInfo = Arrays.copyOfRange(ntlmResponse, 16, ntlmResponse.length);
            byte[] responseKeyNT = CifsUtils.nTOWFv2(domain, account, passwd);

            byte[] compute = CifsUtils.computeResponse(responseKeyNT, serverChallenge, authInfo);

            return Arrays.equals(response, compute);
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-nlmp/99d90ff4-957f-4c8a-80e4-5bfe5a9a9832
    public static final int NTLMSSP_NEGOTIATE_UNICODE = 0x00000001;
    public static final int NTLMSSP_NEGOTIATE_OEM = 0x00000002;
    public static final int NTLMSSP_REQUEST_TARGET = 0x00000004;
    public static final int NTLMSSP_NEGOTIATE_SIGN = 0x00000010;
    public static final int NTLMSSP_NEGOTIATE_SEAL = 0x00000020;
    public static final int NTLMSSP_NEGOTIATE_DATAGRAM = 0x00000040;
    public static final int NTLMSSP_NEGOTIATE_LM_KEY = 0x00000080;
    public static final int NTLMSSP_NEGOTIATE_NETWARE = 0x00000100;
    public static final int NTLMSSP_NEGOTIATE_NTLM = 0x00000200;
    public static final int NTLMSSP_NEGOTIATE_NT_ONLY = 0x00000400;
    public static final int NTLMSSP_ANONYMOUS = 0x00000800;
    public static final int NTLMSSP_NEGOTIATE_OEM_DOMAIN_SUPPLIED = 0x00001000;
    public static final int NTLMSSP_NEGOTIATE_OEM_WORKSTATION_SUPPLIED = 0x00002000;
    public static final int NTLMSSP_NEGOTIATE_THIS_IS_LOCAL_CALL = 0x00004000;
    public static final int NTLMSSP_NEGOTIATE_ALWAYS_SIGN = 0x00008000;
    public static final int NTLMSSP_TARGET_TYPE_DOMAIN = 0x00010000;
    public static final int NTLMSSP_TARGET_TYPE_SERVER = 0x00020000;
    public static final int NTLMSSP_TARGET_TYPE_SHARE = 0x00040000;
    public static final int NTLMSSP_NEGOTIATE_EXTENDED_SESSIONSECURITY = 0x00080000;
    public static final int NTLMSSP_NEGOTIATE_IDENTIFY = 0x00100000;
    public static final int NTLMSSP_REQUEST_NON_NT_SESSION_KEY = 0x00400000;
    public static final int NTLMSSP_NEGOTIATE_TARGET_INFO = 0x00800000;
    public static final int NTLMSSP_NEGOTIATE_VERSION = 0x02000000;
    public static final int NTLMSSP_NEGOTIATE_128 = 0x20000000;
    public static final int NTLMSSP_NEGOTIATE_KEY_EXCH = 0x40000000;
    public static final int NTLMSSP_NEGOTIATE_56 = 0x80000000;
}

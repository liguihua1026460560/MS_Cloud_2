package com.macrosan.filesystem.cifs.types.smb2;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import static com.macrosan.filesystem.cifs.reply.smb2.NegprotReply.SERVER_GUID;

public abstract class IOCTLSubReply {
    public abstract int writeStruct(ByteBuf buf, int offset);

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/9ace71ad-a6c1-4565-83d8-4cd9c9a92ea4
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class ValidateNegotiateInfo extends IOCTLSubReply {
        int capabilities;
        byte[] serverGuid = SERVER_GUID;
        short securityMode;
        short dialect;

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            buf.setIntLE(offset, capabilities);
            buf.setBytes(offset + 4, serverGuid);
            buf.setShortLE(offset + 20, securityMode);
            buf.setShortLE(offset + 22, dialect);
            return 24;
        }
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/34a727a2-960a-4825-9cd2-6100c84e3a81
    @EqualsAndHashCode(callSuper = true)
    @Data
    @Accessors(chain = true)
    public static class ObjectId1 extends IOCTLSubReply {
        SMB2FileId fileId;

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            fileId.writeStruct(buf, offset);
            fileId.writeStruct(buf, offset + 32);
            return 64;
        }
    }


    public static class NetworkInterfaceInfo extends IOCTLSubReply {
        public List<Interface> interfaceList = new LinkedList<>();

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            ListIterator<Interface> listIterator = interfaceList.listIterator();
            int start = offset;
            while (listIterator.hasNext()) {
                Interface inter = listIterator.next();

                if (listIterator.hasNext()) {
                    buf.setIntLE(offset, Interface.size);
                } else {
                    buf.setIntLE(offset, 0);
                }

                buf.setIntLE(offset + 4, inter.ifIndex);
                buf.setIntLE(offset + 8, inter.capability);
                buf.setLongLE(offset + 16, inter.speed);

                inter.ip.writeStruct(buf, offset + 24);

                offset += Interface.size;
            }

            return offset - start;
        }

        @AllArgsConstructor
        public static class Interface {
            int ifIndex;
            int capability;
            long speed;

            IP ip;

            public static int size = 152;
        }

        private abstract static class IP {
            abstract int writeStruct(ByteBuf buf, int offset);
        }

        @AllArgsConstructor
        public static class IPV4 extends IP {
            short port;
            int ip;

            @Override
            int writeStruct(ByteBuf buf, int offset) {
                //InterNetwork
                buf.setShortLE(offset, 0x0002);
                buf.setShortLE(offset + 2, port);
                buf.setInt(offset + 4, ip);
                return 8;
            }

            public static int getIP(String ip) {
                String[] split = ip.split("\\.");
                int n = 0;
                for (int i = 0; i < 4; i++) {
                    n *= 256;
                    n += Integer.parseInt(split[i]);
                }

                return n;
            }
        }
    }
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class RpcResponseSubReply extends IOCTLSubReply {
        private byte[] responsePDU;

        @Override
        public int writeStruct(ByteBuf buf, int offset) {
            if (responsePDU.length > 0) {
                buf.setBytes(offset, responsePDU);
            }
            return responsePDU.length;
        }
    }
}

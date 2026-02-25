package com.macrosan.filesystem.cifs.types;

import com.macrosan.constants.ErrorNo;
import com.macrosan.utils.msutils.MsException;
import io.netty.buffer.ByteBuf;
import lombok.ToString;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class Spnego {
    public static class SpnegoMessage extends NTLMSSP.NTLMSSPMessage {
        public NTLMSSP.NTLMSSPMessage token;

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            return 0;
        }
    }

    @ToString
    public static class SpnegoTokenInitMessage extends SpnegoMessage {
        public static final byte[] OID_SPNEGO = new byte[]{0x2b, 0x06, 0x01, 0x05, 0x05, 0x02};
        public static final byte[] OID_KERBEROS5 = new byte[]{0x2a, (byte) 0x86, 0x48, (byte) 0x86, (byte) 0xf7, 0x12, 0x01, 0x02, 0x02};
        public static final byte[] OID_MS_KERBEROS5 = new byte[]{0x2a, (byte) 0x86, 0x48, (byte) 0x82, (byte) 0xf7, 0x12, 0x01, 0x02, 0x02};
        public static final byte[] OID_NTLMSSP = new byte[]{0x2b, 0x06, 0x01, 0x04, 0x01, (byte) 0x82, 0x37, 0x02, 0x02, 0x0a};
        byte[] oid;
        byte[][] metchType;
        public byte[] krbData = new byte[0];

        @Override

        public int readStruct(ByteBuf buf, int offset) {
            int[] curOffset = new int[]{offset};
            int msgLen = asn1ReadTag((byte) 0x60, buf, curOffset);
            if (msgLen < 0) {
                return -1;
            }

            msgLen += curOffset[0] - offset;

            int oidLen = asn1ReadTag((byte) 0x06, buf, curOffset);
            if (oidLen < 0) {
                return -1;
            }

            oid = new byte[oidLen];
            buf.getBytes(curOffset[0], oid);
            curOffset[0] += oidLen;
            if (Arrays.equals(OID_KERBEROS5, oid)) {
                krbData = new byte[msgLen];
                buf.getBytes(offset, krbData);
                return msgLen;
            }
            if (!Arrays.equals(OID_SPNEGO, oid)) {
                return -1;
            }

            asn1ReadTag((byte) 0xa0, buf, curOffset);
            asn1ReadTag((byte) 0x30, buf, curOffset);

            while (curOffset[0] - offset < msgLen) {
                byte type = buf.getByte(curOffset[0]);
                switch (type) {
                    //0xa0
                    case -96:
                        asn1ReadTag((byte) 0xa0, buf, curOffset);
                        int metchTotalLen = asn1ReadTag((byte) 0x30, buf, curOffset);
                        List<byte[]> metchTypeList = new LinkedList<>();
                        while (metchTotalLen > 0) {
                            int metchLen = asn1ReadTag((byte) 0x06, buf, curOffset);
                            byte[] metch = new byte[metchLen];
                            buf.getBytes(curOffset[0], metch);
                            curOffset[0] += metchLen;
                            metchTypeList.add(metch);
                            metchTotalLen -= 2 + metchLen;
                        }

                        metchType = metchTypeList.toArray(new byte[0][]);
                        break;
                    //0xa1
                    case -95:
                        break;
                    //0xa2
                    case -94:
                        asn1ReadTag((byte) 0xa2, buf, curOffset);
                        int tokenLen = asn1ReadTag((byte) 0x04, buf, curOffset);
                        token = NTLMSSP.readStruct(buf, curOffset[0]);
                        curOffset[0] += tokenLen;
                        break;
                    //0xa3
                    case -93:
                        break;

                }
            }


            if (curOffset[0] - offset != msgLen) {
                return -1;
            }

            return msgLen;
        }
    }

    public static int asn1ReadTag(byte except, ByteBuf buf, int[] offset) {
        if (buf.getByte(offset[0]) != except) {
            return -1;
        }

        offset[0]++;
        byte b = buf.getByte(offset[0]);
        offset[0]++;
        if ((b & 0x80) != 0) {
            int n = b & 0x7f;
            int len = buf.getByte(offset[0]) & 0xff;
            offset[0]++;

            while (n > 1) {
                b = buf.getByte(offset[0]);
                offset[0]++;
                len = (len << 8) | (b & 0xff);
                n--;
            }

            return len;
        } else {
            return b;
        }
    }

    public static class KrbAqRep extends SpnegoMessage {
        public byte[] token;
        public int writeStruct(ByteBuf buf, int offset) {
            buf.setBytes(offset, token);
            return token.length;
        }

        @Override
        public int size() {
            return token.length;
        }
    }

    public static class SpnegoTokenTargMessage extends SpnegoMessage {
        public static final byte[] GSS_NTLM_MECHANISM = new byte[]{0x2b, 0x06, 0x01, 0x04, 0x01, (byte) 0x82, 0x37, 0x02, 0x02, 0x0a};
        public static final byte[] OID_KERBEROS5 = new byte[]{0x2a, (byte) 0x86, 0x48, (byte) 0x86, (byte) 0xf7, 0x12, 0x01, 0x02, 0x02};
        public static final byte[] OID_MS_KERBEROS5 = new byte[]{0x2a, (byte) 0x86, 0x48, (byte) 0x82, (byte) 0xf7, 0x12, 0x01, 0x02, 0x02};

        public byte res;
        public byte[] supportedMech;
        public byte[] mic;

        @Override
        public int readStruct(ByteBuf buf, int offset) {
            int[] curOffset = new int[]{offset};
            int msgLen = asn1ReadTag((byte) 0xa1, buf, curOffset);
            if (msgLen < 0) {
                return -1;
            }

            msgLen += curOffset[0] - offset;

            int len = asn1ReadTag((byte) 0x30, buf, curOffset);
            if (len < 0) {
                return -1;
            }

            while (curOffset[0] - offset < msgLen) {
                byte type = buf.getByte(curOffset[0]);
                switch (type) {
                    case (byte) 0xa0:
                        int resLen = asn1ReadTag((byte) 0xa0, buf, curOffset);
                        if (resLen < 0) {
                            return -1;
                        }

                        resLen = asn1ReadTag((byte) 0x0a, buf, curOffset);
                        if (resLen != 1) {
                            return -1;
                        }

                        res = buf.getByte(curOffset[0]);
                        curOffset[0]++;
                        break;
                    case (byte) 0xa2:
                        int tokenLen = asn1ReadTag((byte) 0xa2, buf, curOffset);
                        if (tokenLen < 0) {
                            return -1;
                        }

                        tokenLen = asn1ReadTag((byte) 0x04, buf, curOffset);
                        if (tokenLen < 0) {
                            return -1;
                        }

                        token = NTLMSSP.readStruct(buf, curOffset[0]);
                        curOffset[0] += tokenLen;
                        break;
                    case (byte) 0xa3:
                        int micLen = asn1ReadTag((byte) 0xa3, buf, curOffset);
                        if (micLen < 0) {
                            return -1;
                        }

                        micLen = asn1ReadTag((byte) 0x04, buf, curOffset);
                        if (micLen < 0) {
                            return -1;
                        }

                        mic = new byte[micLen];
                        buf.getBytes(curOffset[0], mic);
                        curOffset[0] += micLen;
                        break;
                    default:
                        return -1;
                }
            }

            return msgLen;
        }


        public int writeStruct(ByteBuf buf, int offset) {
            int[] totalLen = new int[]{0};

            int[] micLen = null;
            if (mic != null) {
                micLen = new int[]{mic.length, 0};
                asn1MarkTag(micLen);
                micLen[1] = micLen[0];
                asn1MarkTag(micLen);
                totalLen[0] += micLen[0];
            }

            int[] len = null;
            if (token != null) {
                len = new int[]{token.size(), 0};
                asn1MarkTag(len);
                len[1] = len[0];
                asn1MarkTag(len);
                totalLen[0] += len[0];
            }

            int[] mechLen = null;
            if (supportedMech != null) {
                mechLen = new int[]{supportedMech.length, 0};
                asn1MarkTag(mechLen);
                mechLen[1] = mechLen[0];
                asn1MarkTag(mechLen);
                totalLen[0] += mechLen[0];
            }

            //res len
            totalLen[0] += 5;

            asn1MarkTag(totalLen);

            int writeLen = 0;
            writeLen += asn1WriteTag((byte) 0xa1, buf, offset + writeLen, totalLen[0]);
            writeLen += asn1WriteTag((byte) 0x30, buf, offset + writeLen, totalLen[0] - writeLen);

            writeLen += asn1WriteTag((byte) 0xa0, buf, offset + writeLen, 3);
            writeLen += asn1WriteTag((byte) 0x0a, buf, offset + writeLen, 1);
            buf.setByte(offset + writeLen, res);
            writeLen++;

            if (supportedMech != null) {
                writeLen += asn1WriteTag((byte) 0xa1, buf, offset + writeLen, mechLen[1]);
                writeLen += asn1WriteTag((byte) 0x06, buf, offset + writeLen, supportedMech.length);
                buf.setBytes(offset + writeLen, supportedMech);
                writeLen += supportedMech.length;
            }

            if (token != null) {
                writeLen += asn1WriteTag((byte) 0xa2, buf, offset + writeLen, len[1]);
                writeLen += asn1WriteTag((byte) 0x04, buf, offset + writeLen, token.size());
                writeLen += token.writeStruct(buf, offset + writeLen);
            }

            if (mic != null) {
                writeLen += asn1WriteTag((byte) 0xa3, buf, offset + writeLen, micLen[1]);
                writeLen += asn1WriteTag((byte) 0x04, buf, offset + writeLen, mic.length);
                buf.setBytes(offset + writeLen, mic);
                writeLen += mic.length;
            }

            return writeLen;
        }

        public static final byte SPNEGO_ACCEPT_COMPLETED = 0;
        public static final byte SPNEGO_ACCEPT_INCOMPLETE = 1;
        public static final byte SPNEGO_REJECT = 2;
        public static final byte SPNEGO_REQUEST_MIC = 3;
    }

    public static void asn1MarkTag(int[] len) {
        if (len[0] < 128) {
            len[0] += 2;
        } else if (len[0] < 256) {
            len[0] += 3;
        } else if (len[0] < 65536) {
            len[0] += 4;
        } else {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "not support len");
        }
    }

    public static int asn1WriteTag(byte tag, ByteBuf buf, int off, int len) {
        buf.setByte(off, tag);

        if (len < 128) {
            buf.setByte(off + 1, len);
            return 2;
        } else {
            byte l = 1;
            int len0 = len;
            while (len0 > 256) {
                l++;
                len0 >>= 8;
            }

            len0 = len;
            buf.setByte(off + 1, l | 0x80);
            for (byte i = 0; i < l; i++) {
                buf.setByte(off + 1 + l - i, len0 & 0xff);
                len0 >>= 8;
            }

            return 2 + l;
        }
    }
}

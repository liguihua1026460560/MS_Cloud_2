package com.macrosan.filesystem.cifs.reply.smb1;

import com.macrosan.filesystem.cifs.SMB1Body;
import com.macrosan.filesystem.utils.CifsUtils;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.log4j.Log4j2;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Random;

/**
 * https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-cifs/a4229e1a-8a4e-489a-a2eb-11b7f360e60c
 */
@Log4j2
@Data
@EqualsAndHashCode(callSuper = true)
public class NegprotReply extends SMB1Body {
    short dialectIndex;
    byte securityMode;
    short maxMpxCount;
    short maxNumberVcs;
    int maxBufferSize;
    int maxRawSize;
    int sessionKey;
    int capabilities;
    long systemTime;
    short serverTimeZone;
    byte[] challenge;

    public static NegprotReply ntlmsspNegprotReply(int choice, short flag2) {
        NTLMSSPNegprotReply reply = new NTLMSSPNegprotReply();
        reply.wordCount = 17;
        reply.byteCount = reply.serverGUID.length + reply.security.length;

        reply.dialectIndex = (short) choice;
        reply.securityMode = NEGOTIATE_SECURITY_USER_LEVEL | NEGOTIATE_SECURITY_CHALLENGE_RESPONSE;
        reply.maxMpxCount = 50;//samba默认值
        reply.maxNumberVcs = 1;
        reply.maxBufferSize = 16644;
        reply.maxRawSize = 65536;
        reply.sessionKey = new Random().nextInt();
        reply.capabilities = CAP_UNICODE | CAP_LARGE_FILES | CAP_NT_SMBS | CAP_RPC_REMOTE_APIS | CAP_STATUS32
                | CAP_W2K_SMBS | CAP_UNIX | CAP_EXTENDED_SECURITY | CAP_LARGE_READX | CAP_LARGE_WRITEX;

        reply.systemTime = CifsUtils.nttime(System.currentTimeMillis());
        //中国的时区
        reply.serverTimeZone = (short) 0xfe20;
        reply.challenge = new byte[0];
        new Random().nextBytes(reply.serverGUID);
        return reply;
    }

    public int writeStruct(ByteBuf buf, int offset) {
        int size = super.writeStruct(buf, offset);

        int start = offset + 1;
        buf.setShortLE(start, dialectIndex);
        buf.setByte(start + 2, securityMode);
        buf.setShortLE(start + 3, maxMpxCount);
        buf.setShortLE(start + 5, maxNumberVcs);
        buf.setIntLE(start + 7, maxBufferSize);
        buf.setIntLE(start + 11, maxRawSize);
        buf.setIntLE(start + 15, sessionKey);
        buf.setIntLE(start + 19, capabilities);
        buf.setLongLE(start + 23, systemTime);
        buf.setShortLE(start + 31, serverTimeZone);
        buf.setByte(start + 33, challenge.length);
        start = offset + 3 + wordCount * 2;
        buf.setBytes(start, challenge);

        return size;
    }

    @Override
    public int readStruct(ByteBuf buf, int offset) {
        int size = super.readStruct(buf, offset);

        //words
        int start = offset + 1;
        dialectIndex = buf.getShortLE(start);
        securityMode = buf.getByte(start + 2);
        maxMpxCount = buf.getShortLE(start + 3);
        maxNumberVcs = buf.getShortLE(start + 5);
        maxBufferSize = buf.getIntLE(start + 7);
        maxRawSize = buf.getIntLE(start + 11);
        sessionKey = buf.getIntLE(start + 15);
        capabilities = buf.getIntLE(start + 19);
        systemTime = buf.getLongLE(start + 23);
        serverTimeZone = buf.getShortLE(start + 31);
        int challengeLength = buf.getByte(start + 33) & 0xff;
        //bytes
        start = offset + 3 + 2 * wordCount;
        challenge = new byte[challengeLength];
        buf.getBytes(start, challenge);

        return size;
    }

    public static final int CAP_RAW_MODE = 0x00000001;
    public static final int CAP_MPX_MODE = 0x00000002;
    public static final int CAP_UNICODE = 0x00000004;
    public static final int CAP_LARGE_FILES = 0x00000008;
    public static final int CAP_NT_SMBS = 0x00000010;
    public static final int CAP_RPC_REMOTE_APIS = 0x00000020;
    public static final int CAP_STATUS32 = 0x00000040;
    public static final int CAP_LEVEL_II_OPLOCKS = 0x00000080;
    public static final int CAP_LOCK_AND_READ = 0x00000100;
    public static final int CAP_NT_FIND = 0x00000200;
    public static final int CAP_DFS = 0x00001000;
    public static final int CAP_W2K_SMBS = 0x00002000;
    public static final int CAP_LARGE_READX = 0x00004000;
    public static final int CAP_LARGE_WRITEX = 0x00008000;
    public static final int CAP_LWIO = 0x00010000;
    public static final int CAP_UNIX = 0x00800000;
    public static final int CAP_DYNAMIC_REAUTH = 0x20000000;
    public static final int CAP_EXTENDED_SECURITY = 0x80000000;

    public static final int NEGOTIATE_SECURITY_USER_LEVEL = 0x01;
    public static final int NEGOTIATE_SECURITY_CHALLENGE_RESPONSE = 0x02;
    public static final int NEGOTIATE_SECURITY_SIGNATURES_ENABLED = 0x04;
    public static final int NEGOTIATE_SECURITY_SIGNATURES_REQUIRED = 0x08;

    @Override
    public String toString() {
        String res = "NegprotReply{" +
                "dialectIndex=" + dialectIndex +
                ", securityMode=" + securityMode +
                ", maxMpxCount=" + maxMpxCount +
                ", maxNumberVcs=" + maxNumberVcs +
                ", maxBufferSize=" + maxBufferSize +
                ", maxRawSize=" + maxRawSize +
                ", sessionKey=" + sessionKey +
                ", capabilities=";

        Field[] fields = this.getClass().getDeclaredFields();

        for (Field field : fields) {
            field.setAccessible(true);
            int modifies = field.getModifiers();
            if (Modifier.isStatic(modifies) && Modifier.isFinal(modifies) && field.getName().startsWith("CAP_")) {
                try {
                    int value = (int) field.get(null);
                    if ((capabilities & value) != 0) {
                        res += field.getName() + " ";
                    }
                } catch (IllegalAccessException e) {
                    log.error("", e);
                }
            }
        }

        res += ", systemTime=" + systemTime +
                ", serverTimeZone=" + serverTimeZone +
                ", challenge=" + Arrays.toString(challenge) +
                '}';

        return res;
    }
}

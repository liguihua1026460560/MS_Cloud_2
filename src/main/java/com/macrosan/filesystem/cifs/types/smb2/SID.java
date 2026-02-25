package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.nfs.NFSException;
import com.macrosan.filesystem.utils.acl.CIFSACL;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;

import java.util.LinkedList;
import java.util.List;

import static com.macrosan.filesystem.FsConstants.NfsErrorNo.NFS3ERR_PERM;


/**
 * @Author: WANG CHENXING
 * @Date: 2025/5/14
 * @Description:https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/f992ad60-0fe4-4b87-9fed-beb478836861
 */

@EqualsAndHashCode()
@Data
@Accessors(chain = true)
@Log4j2
public class SID {
    //0x01
    byte revision = 1;
    //max 15，表示subAuthority数组中的subAuthority数量，如subAuthority: 21-2112963147-2331644088-1633285465-1000，则为5
    byte numSubAuth;
    //6bytes，如果authority小于2^32，则以10进制表示，不可加0前缀；若大于等于2^32，则以16进制表示，必须转为字符串，且前缀加0x
    Authority authority = new Authority();
    SubAuthority subAuthority = new SubAuthority();
    String displayName = null;

    //必须是4的倍数
    public int writeStruct(ByteBuf buf, int offset) {
        buf.setByte(offset, revision);
        buf.setByte(offset + 1, numSubAuth);
        authority.writeStruct(buf, offset + 2);
        return 8 + subAuthority.writeStruct(buf, offset + 8);
    }

    /**
     * 返回当前SID占用的字节数
     **/
    public int readStruct(ByteBuf buf, int offset) {
        revision = buf.getByte(offset);
        numSubAuth = buf.getByte(offset + 1);
        authority.readStruct(buf, offset + 2);
        subAuthority.readStruct(buf, offset + 8);
        if (CIFSACL.cifsACL) {
            log.info("【SID】 readStruct: revision: {}, numSubAuth: {}, auth: {}, subAuth: {}", revision, numSubAuth, authority, subAuthority);
        }
//        displayName = convertSIDToString(this);
        return 8 + numSubAuth * 4;
    }

    public SID setRevision(int revision) {
        this.revision = (byte) revision;
        return this;
    }

    public SID setNumSubAuth(int numSubAuth) {
        this.numSubAuth = (byte) numSubAuth;
        return this;
    }

    public SID setAuthority(long SIDValue) {
        authority.SIDValue = SIDValue;
        return this;
    }

    public SID setSubAuth(List<Long> list) {
        subAuthority.subAutList = list;
        return this;
    }

    public static String convertSIDToString(SID sid) {
        StringBuilder builder = new StringBuilder("S-1");
        try {
            builder.append("-").append(sid.authority.SIDValue);
            List<Long> subAuthList = sid.getSubAuthority().getSubAutList();
            for (byte i = 0; i < subAuthList.size(); i++) {
                builder.append("-").append(subAuthList.get(i));
            }
        } catch (Exception e) {
            log.error("convert sid error, sid: {}", sid, e);
        }

        return builder.toString();
    }

    public static SID generateSID(String sidStr) {
        SID sid = new SID();
        //subAuth: S-1-3-1 --> S-1-identifier-subIdentifier
        String[] authArr = sidStr.split("-");
        sid.numSubAuth = (byte) (authArr.length - 3);
        if (sid.numSubAuth < 0) {
            throw new NFSException(NFS3ERR_PERM, "generateSID error, " + sidStr);
        }
        sid.authority.SIDValue = Long.parseLong(authArr[2]);
        LinkedList<Long> subAuthList = new LinkedList<>();
        for (byte i = 0; i < sid.numSubAuth; i++) {
            subAuthList.add(Long.parseLong(authArr[3+i]));
        }
        sid.subAuthority.subAutList = subAuthList;
        sid.displayName = sidStr;

        return sid;
    }

    public SID clone() {
        SID sid = new SID();
        sid.revision = this.revision;
        sid.numSubAuth = this.numSubAuth;
        Authority auth = new Authority();
        auth.SIDValue = this.authority.SIDValue;
        sid.authority = auth;
        sid.subAuthority.subAutList = new LinkedList<>();
        sid.subAuthority.subAutList.addAll(this.subAuthority.subAutList);
        return sid;
    }

    @Data
    public static class Authority {
        public long SIDValue;
        //2^32次方，int类型最大为2^32-1
        public static long THRESHOLD = 1L << 32;

        /**
         * 把long型数值转换为小端序的6字节数值
         **/
        public int writeStruct(ByteBuf buf, int offset) {
            buf.setBytes(offset, convertLongToByte(SIDValue, SIDValue < THRESHOLD));
            return 6;
        }

        /**
         * 把小端序的6字节数值转换为long型数值
         **/
        public int readStruct(ByteBuf buf, int offset) {
            //抓包发现authority没有使用小端序
            byte[] bytesLE = new byte[6];
            buf.getBytes(offset, bytesLE);
            long result = 0L;
            for (int i = 0; i < 6; i++) {
                // 将字节转换为无符号整数（0~255）
                int unsignedByte = bytesLE[i] & 0xFF;
                // 左移至对应位置（第一个字节左移40位，第二个32位, 24, 16, 8...）
                long shiftedValue = (long) unsignedByte << (40 - i * 8);
                result |= shiftedValue; // 合并到结果中
            }

            SIDValue = result;
            return 6;
        }
    }

    @Data
    public class SubAuthority {
        //虽然subAuth是32-bit，但是java中int类型是有符号数值，而实际的数值可能超过有符号正数的上限，因此采用long型
        public List<Long> subAutList;

        public int writeStruct(ByteBuf buf, int offset) {
            int off = 0;
            if (null != subAutList) {
                for (long subAut : subAutList) {
                    //需要截取long型数值的低32bit，首先获取每一位
                    byte[] byteArray = new byte[4];
                    byteArray[0] = (byte)(subAut >> 24);
                    byteArray[1] = (byte)(subAut >> 16);
                    byteArray[2] = (byte)(subAut >> 8);
                    byteArray[3] = (byte) subAut;

                    //按小端序重排，即byteArray[4]->第一个，3->第二个，2->第3个，1 -> 最后一个
                    byte[] array = new byte[4];
                    array[0] = byteArray[3];
                    array[1] = byteArray[2];
                    array[2] = byteArray[1];
                    array[3] = byteArray[0];

                    buf.setBytes(offset + off, array);
                    off += 4;
                    byteArray = null;
                    array = null;
                }
            }
            return off;
        }

        /**
         * 根据sub数组中的子签名的个数，从buf解析子签名至subAutList中
         *
         **/
        public int readStruct(ByteBuf buf, int offset) {
            if (numSubAuth > 0) {
                subAutList = new LinkedList<>();
                for (byte i = 0; i < numSubAuth; i++) {
//                    long subAuth = buf.getIntLE(offset + i * 4);
                    long subAuth = convertByteToLong(buf, offset + i * 4, 4);
                    subAutList.add(subAuth);
                }
            }

            return numSubAuth * 4;
        }
    }

    /**
     * 把long型数值转换为小端序的6字节数值
     **/
    public static byte[] convertLongToByte(long value, boolean isNotEnough) {
        byte[] bytes = new byte[6];
        long longValue = isNotEnough? value & 0xFFFFFFFFL : value;

        //小端序，value高位在数组最右边，value低位在数组最左边
        for (int i = 0; i < 6; i++) {
            bytes[5 - i] = (byte)(longValue >> (i * 8));
        }

        return bytes;
    }

    /**
     * 把小端序数组转成long值
     * 0x12345678，左边是低地址、高位字节；右边是高地址，低位字节
     * 大端序：12、34、56、78
     * 小端序：78、56、34、12
     **/
    public static long convertByteToLong(ByteBuf buf, int offset, int length) {
        byte[] bytesLE = new byte[length];
        buf.getBytes(offset, bytesLE);
        long res = 0;
        for (int i = 0; i < bytesLE.length; i++) {
            //与0xFFL确保无符号8位整数
            res |= (bytesLE[i] & 0xFFL) << (8*i);
        }

        return res;
    }

    public int size() {
        int res = 8;
        if (subAuthority.subAutList != null) {
            res += subAuthority.subAutList.size() * 4;
        }
        return res;
    }

    //https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/81d92bba-d22b-4a8c-908a-554ab29148ab
    //空SID，没有成员的组，当SID值未知时，通常使用该值
    public static SID NONE_SID = SID.generateSID("S-1-0-0");
    //所有人
    public static SID EVERYONE = SID.generateSID("S-1-1-0");
    //本地
    public static SID LOCAL = SID.generateSID("S-1-2-0");
    //控制台
    public static SID CONSOLE = SID.generateSID("S-1-2-1");
    //创建者所有者ID
    public static SID CREATE_OWNER = SID.generateSID("S-1-3-0");
    //创建者组ID
    public static SID CREATE_GROUP = SID.generateSID("S-1-3-1");
    //域管理员账户组 <S-1-5-21-<domain>-512>，当前默认domain=1
    public static SID DOMAIN_ADMINS = SID.generateSID("S-1-5-21-1-512");
    //域用户组 <S-1-5-21-<domain>-513>
    public static SID DOMAIN_USERS = SID.generateSID("S-1-5-21-1-513");
    //域访客 <S-1-5-21-<domain>-514>
    public static SID DOMAIN_GUESTS = SID.generateSID("S-1-5-21-1-514");
    //匿名用户
    public static SID ANONYMOUS = SID.generateSID("S-1-5-7");

}

package com.macrosan.filesystem.utils;

import com.macrosan.constants.ErrorNo;
import com.macrosan.filesystem.cifs.types.smb2.pipe.RpcPipeType;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.utils.msutils.MsException;
import io.netty.buffer.ByteBuf;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.digest.HmacUtils;
import org.apache.commons.lang3.StringUtils;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Optional;

import static com.macrosan.constants.ServerConstants.DEFAULT_DATA;
import static com.macrosan.filesystem.FsConstants.*;
import static com.macrosan.filesystem.cifs.call.smb2.CreateCall.FILE_DIRECTORY_FILE;
import static com.macrosan.filesystem.cifs.call.smb2.CreateCall.FILE_NON_DIRECTORY_FILE;
import static com.macrosan.filesystem.cifs.reply.smb1.QueryFileUnixReply.*;
import static com.macrosan.filesystem.quota.FSQuotaConstants.SMB_CAP_QUOTA_FILE_NAME;
import static java.nio.charset.StandardCharsets.UTF_16LE;

@Log4j2
public class CifsUtils {
    private static final long TIME_FIXUP_CONSTANT = 116444736_000_000_000L;

    // 时间戳转nt时间格式
    public static long nttime(long stamp) {
        return stamp * 10000 + TIME_FIXUP_CONSTANT;
    }

    public static void writeChars(ByteBuf buf, int off, char[] chars) {
        for (int i = 0; i < chars.length; i++) {
            buf.setShortLE(off, chars[i]);
            off += 2;
        }

        buf.setShortLE(off, 0);
    }

    public static char[] readChars(ByteBuf buf, int off) {
        int end = off;
        while (buf.getChar(end) != 0) {
            end += 2;
        }

        char[] res = new char[(end - off) / 2];
        for (int i = 0; i < res.length; i++) {
            res[i] = (char) buf.getShortLE(off);
            off += 2;
        }

        return res;
    }

    public static byte[] readBytes(ByteBuf buf, int offset, int index) {
        int size = buf.getShortLE(offset + index) & 0xffff;
        byte[] res = new byte[size];
        if (size > 0) {
            int off = buf.getIntLE(offset + index + 4);
            buf.getBytes(offset + off, res);
        }

        return res;
    }

    private static ThreadLocal<MessageDigest> MD4_DIGEST = ThreadLocal.withInitial(() -> DigestUtils.getDigest("MD4"));

    public static byte[] nTOWFv2(String domain, String username, String password) {
        MessageDigest md4 = MD4_DIGEST.get();
        md4.update(password.getBytes(UTF_16LE));
        HMACT64 hmac = new HMACT64(md4.digest());
        hmac.update(username.toUpperCase().getBytes(UTF_16LE));
        hmac.update(domain.getBytes(UTF_16LE));
        return hmac.digest();
    }

    public static byte[] computeResponse(byte[] responseKey,
                                         byte[] serverChallenge,
                                         byte[] clientData) {
        HMACT64 hmac = new HMACT64(responseKey);
        hmac.update(serverChallenge);
        hmac.update(clientData);
        return hmac.digest();
    }

    public static class HMACT64 {
        private static final int BLOCK_LENGTH = 64;

        private static final byte IPAD = (byte) 0x36;

        private static final byte OPAD = (byte) 0x5c;

        private final MessageDigest md5;

        private final byte[] ipad = new byte[BLOCK_LENGTH];

        private final byte[] opad = new byte[BLOCK_LENGTH];

        public HMACT64(byte[] key) {
            int length = Math.min(key.length, BLOCK_LENGTH);
            for (int i = 0; i < length; i++) {
                ipad[i] = (byte) (key[i] ^ IPAD);
                opad[i] = (byte) (key[i] ^ OPAD);
            }
            for (int i = length; i < BLOCK_LENGTH; i++) {
                ipad[i] = IPAD;
                opad[i] = OPAD;
            }

            md5 = DigestUtils.getDigest("MD5");
            md5.update(ipad);
        }

        public byte[] digest() {
            byte[] digest = md5.digest();
            md5.update(opad);
            return md5.digest(digest);
        }

        public void update(byte[] input) {
            md5.update(input);
        }
    }

    public static byte[] rc4Encrypt(byte[] key, byte[] text) {
        try {
            Cipher cipher = Cipher.getInstance("RC4");
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "RC4"));
            return cipher.doFinal(text);
        } catch (Exception e) {
            log.error("", e);
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "load rc4 encrypt fail");
        }
    }

    public static byte[] rc4Encrypt(Cipher cipher, byte[] text) {
        try {
            return cipher.update(text);
        } catch (Exception e) {
            log.error("", e);
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "load rc4 encrypt fail");
        }
    }

    private static final ThreadLocal<Mac> SHA256_MAC = ThreadLocal.withInitial(() -> {
        try {
            return Mac.getInstance("hmacSHA256");
        } catch (NoSuchAlgorithmException e) {
            log.error("", e);
            return null;
        }
    });

    public static byte[] hmacSHA256(byte[] key, byte[]... data) {
        Mac mac = SHA256_MAC.get();
        byte[] ret;
        try {
            mac.init(new SecretKeySpec(key, "hmacSHA256"));
            for (int i = 0; i < data.length; i++) {
                mac.update(data[i]);
            }
            ret = mac.doFinal();
        } catch (Exception e) {
            log.error("calculate hmac sha256 fail :" + e);
            return DEFAULT_DATA;
        } finally {
            mac.reset();
        }
        return Arrays.copyOf(ret, 16);
    }

    public static int transToUnixMode(int mode) {
        int type = mode & S_IFMT;
        switch (type) {
            case S_IFREG:
                return UNIX_TYPE_FILE;
            case S_IFDIR:
                return UNIX_TYPE_DIR;
            case S_IFLNK:
                return UNIX_TYPE_SYMLINK;
            case S_IFBLK:
                return UNIX_TYPE_BLKDEV;
            case S_IFCHR:
                return UNIX_TYPE_CHARDEV;
            case S_IFFIFO:
                return UNIX_TYPE_FIFO;
            case S_IFSOCK:
                return UNIX_TYPE_SOCKET;
            default:
                return UNIX_TYPE_UNKNOWN;
        }
    }

    public static long SMBTimeToStamp(long smbTime) {
        return (smbTime - TIME_FIXUP_CONSTANT) / 10000_000L;
    }

    public static long getSMBTimeNano(long smbTime) {
        return (smbTime % (1000 * 1000 * 10)) * 100;
    }

    public static long getAllocationSize(int mode, long size) {
        if ((mode & S_IFMT) == S_IFLNK) {
            return 0;
        } else {
            return (size + 4095) & ~4095;
        }
    }

    /**
     * 解析得到父目录的 key
     *
     * @param obj dir/subDir/file
     * @return dir/subDir
     **/
    public static String getParentDirName(String obj) {
        String dirName = "";
        if (obj.endsWith(SMB_CAP_QUOTA_FILE_NAME) || obj.endsWith(SMB_CAP_QUOTA_FILE_NAME + "/")) {
            int i = obj.indexOf(SMB_CAP_QUOTA_FILE_NAME);
            if (i > 0) {
                dirName = obj.substring(0, i);
            }
            return dirName;
        }
        String[] dirPrefix = obj.split("/");

        if (dirPrefix.length > 1) {
            if (obj.endsWith("/")) {
                dirName = obj.substring(0, obj.length() - dirPrefix[dirPrefix.length - 1].length() - 2);
            } else {
                dirName = obj.substring(0, obj.length() - dirPrefix[dirPrefix.length - 1].length() - 1);
            }
        }
        return dirName;
    }

    public static int getCIFSMode(int createOptions, int reqMode) {
        int cifsMode = 0;
        reqMode &= ~FILE_ATTRIBUTE_NORMAL;
        // file
        if ((createOptions & FILE_DIRECTORY_FILE) == 0) {
            if (reqMode == 0) {
                cifsMode = FILE_ATTRIBUTE_ARCHIVE;
            } else {
                cifsMode = reqMode;
            }
            // dir
        } else {
            if (reqMode == 0) {
                cifsMode = FILE_ATTRIBUTE_DIRECTORY;
            } else {
                cifsMode = reqMode | FILE_ATTRIBUTE_DIRECTORY;
            }
        }
        return cifsMode;
    }

    public static int changeToHiddenCifsMode(String newObj, int oldCifsMode, boolean optHidden) {
        int cifsMode = oldCifsMode;
        boolean hasChange = false;
        if (StringUtils.isNotBlank(newObj)) {
            Optional<RpcPipeType> rpcTypeOpt = RpcPipeType.fromFileName(newObj);
            if (rpcTypeOpt.isPresent()){
                cifsMode = FILE_ATTRIBUTE_PIPE_ACCESS_MASK;
                hasChange = true;
            } else {
                if (newObj.endsWith("/")) {
                    cifsMode |= FILE_ATTRIBUTE_DIRECTORY;
                    if ((cifsMode & FILE_ATTRIBUTE_NORMAL) == 0) {
                        cifsMode = cifsMode & ~FILE_ATTRIBUTE_HIDDEN;
                    }
                    hasChange = true;
                } else if (!".".equals(newObj) && !"..".equals(newObj)) {
                    cifsMode |= FILE_ATTRIBUTE_ARCHIVE;
                }
                if (optHidden && !".".equals(newObj) && !"..".equals(newObj)) {
                    String[] split = newObj.split("/");
                    if (split[split.length - 1].startsWith(".")) {
                        cifsMode |= FILE_ATTRIBUTE_HIDDEN;
                        hasChange = true;
                    }
                }
            }
        }
        return hasChange ? cifsMode : oldCifsMode;
    }

    public static int getNFSMode(int createOptions) {
        if ((createOptions & FILE_DIRECTORY_FILE) == 0) {
            return DEFAULT_FILE_MODE | S_IFREG;
        } else {
            return DEFAULT_DIR_MODE | S_IFDIR;
        }
    }

    public static void setDefaultCifsMode(Inode inode) {
        if (inode.getCifsMode() == 0 && StringUtils.isNotBlank(inode.getObjName())) {
            if (inode.getObjName().endsWith("/")) {
                inode.setCifsMode(FILE_ATTRIBUTE_DIRECTORY);
            } else {
                inode.setCifsMode(FILE_ATTRIBUTE_ARCHIVE);
            }
        }
    }


    public static boolean isSMBDir(int createOptions, int reqMode) {
        if ((createOptions & FILE_NON_DIRECTORY_FILE) == FILE_NON_DIRECTORY_FILE) {
            return false;
        }
        return true;
    }


    public static byte[] objNameToSMB(String objName) {
        if (objName.endsWith("/")) {
            objName = objName.substring(0, objName.length() - 1);
        }
        objName = "\\" + objName;

        byte[] res = objName.getBytes(UTF_16LE);
        for (int i = 0; i < res.length; i++) {
            if (res[i] == '/') {
                res[i] = '\\';
            }
        }

        return res;
    }

    /**
     * 判断字符串是否符合模板
     *
     * @param s 待比较字符串
     * @param p 模板(包括*和?)
     */
    public static boolean isMatch(String s, String p) {
        // 字符串s的下标
        int i = 0;
        // 字符串p的下标
        int j = 0;
        int starIndex = -1;
        int iIndex = -1;

        while (i < s.length()) {
            if (j < p.length() && (p.charAt(j) == '?' || p.charAt(j) == s.charAt(i))) {
                ++i;
                ++j;
            } else if (j < p.length() && p.charAt(j) == '*') {
                starIndex = j;
                iIndex = i;
                j++;
            } else if (starIndex != -1) {
                j = starIndex + 1;
                i = iIndex + 1;
                iIndex++;
            } else {
                return false;
            }
        }
        while (j < p.length() && p.charAt(j) == '*') {
            ++j;
        }
        return j == p.length();
    }

    private static final ThreadLocal<Mac> AES_MAC = ThreadLocal.withInitial(() -> {
        try {
            return Mac.getInstance("AESCMAC");
        } catch (NoSuchAlgorithmException e) {
            log.error("", e);
            return null;
        }
    });

    //根据key的长度，分为AES128、AES192、AES256等
    public static byte[] cmacAES(byte[] key, byte[] data) {
        Mac mac = AES_MAC.get();
        byte[] ret;
        try {
            mac.init(new SecretKeySpec(key, "AESCMAC"));
            ret = mac.doFinal(data);
        } catch (Exception e) {
            log.error("calculate cmac aes fail :" + e);
            return DEFAULT_DATA;
        } finally {
            mac.reset();
        }
        return Arrays.copyOf(ret, 16);
    }

    public static byte[] ntlmv2Key(byte[] sessionKey, String constant) {
        MessageDigest digest = DigestUtils.getDigest("MD5");
        digest.update(sessionKey);
        digest.update(constant.getBytes());
        digest.update(new byte[]{0});
        return digest.digest();
    }

    public static byte[] getMIC(byte[] signKey, byte[] sealKey) {
        Mac mac = HmacUtils.getInitializedMac("HMACMD5", signKey);
        //seqNum
        mac.update(new byte[]{0, 0, 0, 0});
        byte[] pdu = new byte[]{0x30, 0x0c, 0x06, 0x0a, 0x2b, 0x06, 0x01, 0x04, 0x01, (byte) 0x82, 0x37, 0x02, 0x02, 0x0a};
        byte[] macFinal = mac.doFinal(pdu);
        byte[] mic = new byte[16];
        mic[0] = 1;//version

        byte[] encrypt = CifsUtils.rc4Encrypt(sealKey, Arrays.copyOf(macFinal, 8));
        System.arraycopy(encrypt, 0, mic, 4, 8);
        //seqNum = 0
        return mic;
    }

    public static String getPathName(String fileName) {
        if (StringUtils.isBlank(fileName)) {
            return "";
        }

        if (fileName.charAt(fileName.length() - 1) == '/') {
            fileName = fileName.substring(0, fileName.length() - 1);
        }


        int end = fileName.lastIndexOf('/');
        String pathName;
        if (end > 0) {
            pathName = fileName.substring(0, end + 1);
        } else {
            pathName = "";
        }

        return pathName;
    }

    public static String getFileName(String fileName) {
        if (StringUtils.isBlank(fileName)) {
            return "";
        }

        if (fileName.charAt(fileName.length() - 1) == '/') {
            fileName = fileName.substring(0, fileName.length() - 1);
        }

        int end = fileName.lastIndexOf('/');
        String res;
        if (end > 0) {
            res = fileName.substring(end + 1);
        } else {
            res = fileName;
        }

        return res;
    }

}

package com.macrosan.storage;

import com.macrosan.utils.functional.ImmutableTuple;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;
import java.security.MessageDigest;

/**
 * MsVnodeUtils
 * Vnode相关工具类
 * <p>
 * MessageDigest 对象其实可以做缓存以复用
 *
 * @author liyixin
 * @date 2018/10/28
 */
public class MsVnodeUtils {

    private static ThreadLocal<MessageDigest> digests = ThreadLocal.withInitial(DigestUtils::getSha1Digest);


    private static final int MAX_NUM = 4;

    private MsVnodeUtils() {
    }

    /**
     * 根据桶名字获取目标节点id
     * <p>
     * 原始算法
     * <pre>
     * for (int i = 0; i < MAX_NUM; i++) {
     *      int tmp = bytes[i] < 0 ? bytes[i] + 256 : bytes[i];
     *      不足四位加一个零？
     *      if (tmp < 16) {
     *          builder.append("0");
     *      }
     *      builder.append(tmp);
     * }
     * <pre/>
     *
     * 先用SHA1计算出160 bit (20 byte)的摘要,然后取前四个byte，负数取补码，小于16加一个零
     * @param bucketName 桶名字
     * @param vnodeNum 节点数
     * @return 目标节点id
     */
    public static String getTargetVnodeId(String bucketName, long vnodeNum) {
        MessageDigest digest = digests.get();
        byte[] bytes = DigestUtils.digest(digest, bucketName.getBytes());
        StringBuilder vnodeBuilder = new StringBuilder(16);
        /* 最终只需要8位，所以最多循环四次就行 */
        for (int i = 0; i < MAX_NUM; i++) {
            /* 负数取补码 */
            int tmp = bytes[i] < 0 ? bytes[i] + 256 : bytes[i];
            if (tmp < 16) {
                vnodeBuilder.append("0");
            }
            vnodeBuilder.append(tmp);
        }

        return String.valueOf(Long.parseLong(vnodeBuilder.toString().substring(0, 8), 16) % vnodeNum);
    }

    /**
     * getObjectVnodeId
     * <p>
     * 计算vnodeid和objectname
     * var1 : vnodeid
     * var2 : objectname
     *
     * @param bucketName 桶名字
     * @param objectName 对象名
     * @param vnodeNum   节点数量
     * @return 包含vnodeid和objectname的元组
     */
    static ImmutableTuple<String, String> getObjectVnodeId(String bucketName, String objectName, long vnodeNum) {
        StringBuilder objBuilder = new StringBuilder(40);
        StringBuilder vnodeBuilder = new StringBuilder(16);
        MessageDigest digest = digests.get();
        byte[] bytes = DigestUtils.digest(digest, (bucketName + File.separator + objectName).getBytes());
        for (int i = 0; i < bytes.length; i++) {
            /* 负数取补码 */
            int tmp = bytes[i] < 0 ? bytes[i] + 256 : bytes[i];
            objBuilder.append(Integer.toHexString(tmp));
            /* vnode最多只需要处理四个byte */
            if (i < MAX_NUM) {
                /* 不足四位加一个零？*/
                if (tmp < 16) {
                    vnodeBuilder.append("0");
                }
                vnodeBuilder.append(Integer.toHexString(tmp));
            }
        }
        return new ImmutableTuple<>(String.valueOf(Long.parseLong(vnodeBuilder.toString().substring(0, 8), 16) % vnodeNum),
                objBuilder.toString());
    }
}

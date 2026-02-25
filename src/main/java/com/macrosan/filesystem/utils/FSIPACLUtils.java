package com.macrosan.filesystem.utils;

import com.macrosan.filesystem.nfs.handler.NFSHandler;
import com.macrosan.message.jsonmsg.FSIpACL;
import io.vertx.core.json.Json;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.net.InetAddress;
import java.util.*;

import static com.macrosan.filesystem.FsConstants.NFSAccessAcl.*;

@Slf4j
public class FSIPACLUtils {

    public static final String IP_KEY_PREFIX = "fsIpACL_";

    /**
     * 获取IP地址范围内所有IP
     *
     * @param ipRange 例如 192.168.1.1-100
     * @return List<String>
     */
    public static List<String> getAllIpFromRange(String ipRange) {
        List<String> ipList = new LinkedList<>();
        try {
            String[] ipRangeArr = ipRange.split("-");
            String ipBase = ipRangeArr[0];
            int ipEnd = Integer.parseInt(ipRangeArr[1]);
            int lastIndex = ipBase.lastIndexOf(".");
            int ipStart = Integer.parseInt(ipBase.substring(lastIndex + 1));
            String ipPrefix = ipBase.substring(0, lastIndex);

            for (int i = ipEnd; i >= ipStart; i--) {
                ipList.add(ipPrefix + "." + i);
            }
            return ipList;
        } catch (Exception e) {
            log.error("getAllIpFromRange error", e);
        }
        return ipList;
    }

    public static String convertWildcardToCIDR(String wildcardIP) {

        String[] parts = wildcardIP.split("\\.");
        StringBuilder baseIP = new StringBuilder();
        int maskBits = 0;

        for (int i = 0; i < parts.length; i++) {
            if (parts[i].equals("*")) {
                baseIP.append("0");
                maskBits = i * 8;
                break;
            } else {
                int octet = Integer.parseInt(parts[i]);
                baseIP.append(octet);
                if (i < 3) baseIP.append(".");
            }
        }

        int length = baseIP.toString().split("\\.").length;
        while (length < 4) {
            baseIP.append(".0");
            length++;
        }

        return baseIP + "/" + maskBits;
    }


    public static boolean hasMountAccess(Map<String, String> bucketInfo, String mountIp) {
        Set<String> ipList = getIpInfoSet(bucketInfo);
        if (ipList.isEmpty()) {
            return true;
        }
        for (String ip : ipList) {
            if (hasAccess(ip, mountIp)) {
                return true;
            }
        }
        log.info("bucket {}, ip {} has no access", bucketInfo.getOrDefault("bucket_name", ""), mountIp);
        return false;
    }

    public static boolean isValidPort(Map<String, String> bucketInfo, String ipInfo, int port) {
        FSIpACL fsIpACL = getIpACLByIpKey(bucketInfo, ipInfo);
        if (null == fsIpACL) {
            return false;
        }
        return fsIpACL.getPortMode().equals("insecure") || fsIpACL.getPortMode().equals("secure") && port < 1024;
    }

    public static int getIpRight(Map<String, String> bucketInfo, String ipInfo) {
        FSIpACL fsIpACL = getIpACLByIpKey(bucketInfo, ipInfo);
        if (null == fsIpACL) {
            return 0;
        }
        return fsIpACL.getOptAccess().equals("rw") ? (READ | LOOK_UP | MODIFY | EXTEND | DELETE | EXECUTE) : (READ | LOOK_UP | EXECUTE);
    }

    public static FSIpACL getIpACLByIpKey(Map<String, String> bucketInfo, String ipKey) {
        String ipAclStr = bucketInfo.getOrDefault(ipKey, "");
        if (StringUtils.isBlank(ipAclStr)) {
            return null;
        }
        return Json.decodeValue(ipAclStr, FSIpACL.class);
    }

    public static boolean hasAccess(String ipKey, String clientIp) {
        if (ipKey.contains("/") && isIPInSubnet(clientIp, ipKey)) {
            return true;
        } else if (ipKey.contains("-") && isIPInRange(clientIp, ipKey)) {
            return true;
        } else if (ipKey.contains("*") && isIPInSubnet(clientIp, convertWildcardToCIDR(ipKey))) {
            return true;
        } else {
            return ipKey.equals(clientIp);
        }
    }

    public static boolean isIPInSubnet(String ipAddress, String subnet) {
        try {
            // 解析网段和IP地址
            String[] subnetParts = subnet.split("/");
            String subnetAddress = subnetParts[0];
            int prefixLength = Integer.parseInt(subnetParts[1]);

            // 获取网段和IP的字节数组
            byte[] subnetBytes = InetAddress.getByName(subnetAddress).getAddress();
            byte[] ipBytes = InetAddress.getByName(ipAddress).getAddress();
            // 检查IP和网段长度是否一致(IPv4或IPv6)
            if (subnetBytes.length != ipBytes.length) {
                return false;
            }

            // 计算子网掩码
            byte[] mask = new byte[subnetBytes.length];
            int remainingBits = prefixLength;

            for (int i = 0; i < mask.length; i++) {
                if (remainingBits >= 8) {
                    mask[i] = (byte) 0xFF;
                    remainingBits -= 8;
                } else if (remainingBits > 0) {
                    mask[i] = (byte) (0xFF << (8 - remainingBits));
                    remainingBits = 0;
                } else {
                    mask[i] = 0;
                }
            }

            // 比较网络部分
            for (int i = 0; i < subnetBytes.length; i++) {
                if ((subnetBytes[i] & mask[i]) != (ipBytes[i] & mask[i])) {
                    return false;
                }
            }

            return true;

        } catch (Exception e) {
            log.error("isIPInSubnet error", e);
        }
        return false;
    }

    public static boolean isIPInRange(String ipAddress, String ipRange) {
        try {
            // 解析IP范围
            String[] rangeParts = ipRange.split("-");
            String startIP = rangeParts[0];
            int endValue = Integer.parseInt(rangeParts[1]);

            // 解析起始IP和待检查IP
            String[] startParts = startIP.split("\\.");
            String[] ipParts = ipAddress.split("\\.");

            // 比较前三个部分是否相同
            for (int i = 0; i < 3; i++) {
                if (!startParts[i].equals(ipParts[i])) {
                    return false;
                }
            }

            // 解析并比较第四个部分
            int startValue = Integer.parseInt(startParts[3]);
            int ipValue = Integer.parseInt(ipParts[3]);

            return ipValue >= startValue && ipValue <= endValue;

        } catch (Exception e) {
            log.error("isIPInRange error", e);
        }
        return false;
    }

    public static Set<String> getIpInfoSet(Map<String, String> bucketInfo) {
        Set<String> ipInfoSet = new HashSet<>();
        for (String ipKey : bucketInfo.keySet()) {
            if (ipKey.startsWith(IP_KEY_PREFIX)) {
                ipInfoSet.add(ipKey.split("_")[1]);
            }
        }
        return ipInfoSet;
    }

    public static String getIpKey(String ipInfo) {
        return IP_KEY_PREFIX + ipInfo;
    }

    public static FSIpACL getIpACL(Map<String, String> bucketInfo, String clientIp) {
        boolean returnDefault = true;
        for (String ipKey : bucketInfo.keySet()) {
            if (ipKey.startsWith(IP_KEY_PREFIX)) {
                returnDefault = false;
                String ipInfo = ipKey.split("_")[1];
                if (hasAccess(ipInfo, clientIp)) {
                    return Json.decodeValue(bucketInfo.get(ipKey), FSIpACL.class);
                }
            }
        }
        if (!returnDefault) {
            log.info("bucket {}, ip {} has no access.", bucketInfo.getOrDefault("bucket_name", ""), clientIp);
        }
        return returnDefault ? FSIpACL.DEFAULT_FS_IP_ACL.clone() : null;
    }

    public static String getClientIp(NFSHandler nfsHandler) {
        if (nfsHandler.isUdp) {
            return nfsHandler.address.host();
        } else {
            return nfsHandler.socket.remoteAddress().host();
        }
    }

}

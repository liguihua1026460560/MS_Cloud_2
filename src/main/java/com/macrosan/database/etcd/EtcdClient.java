package com.macrosan.database.etcd;

import com.alibaba.fastjson.JSONObject;
import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.constants.SysConstants;
import com.macrosan.utils.msutils.MsException;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.macrosan.constants.SysConstants.*;

/**
 * @Author chengyinfeng
 * @Date 2019/12/31 17:57
 */
public class EtcdClient extends BaseService {

    private static Logger logger = LogManager.getLogger(EtcdClient.class.getName());

    private static final int TIMEOUT = 30;

    private static final int TRY_TIMES = 3;

    public static String dnsPrefix = pool.getCommand(REDIS_SYSINFO_INDEX).get(DNS_NAME_PREFIX);

    @SuppressWarnings("UnstableApiUsage")
    private static KV getClient;

    static {
        getClient = Client.builder().endpoints("http://127.0.0.1:2379").build().getKVClient();
    }

    public static Client getMasterClient() {
        String masterDnsUuid = pool.getCommand(REDIS_SYSINFO_INDEX).get(MASTER_DNS_UUID);
        Map<String, String> uuidMap = pool.getCommand(REDIS_NODEINFO_INDEX).hgetall(masterDnsUuid);
        String eth4 = uuidMap.get(HEART_ETH1);
        String eth5 = uuidMap.get(HEART_ETH2);
        return Client.builder().endpoints("http://" + eth4 + ":2379", "http://" + eth5 + ":2379").build();
    }

    /**
     * insert etcd
     *
     * @param key   key
     * @param value value
     */
    public static void put(String key, String value) {
        try {
            getClient.put(ByteSequence.from(key.getBytes()), ByteSequence.from(value.getBytes()))
                    .get(SysConstants.TIMEOUT, TimeUnit.MINUTES);
        } catch (Exception e) {
            logger.error("insert {} to etcd error.", key, e);
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "write bucket to etcd error.");
        }
    }

    public static KeyValue get(String key) {
        KeyValue kv = null;
        try {
            GetResponse getResponse = getClient.get(ByteSequence.from(key.getBytes())).get(1, TimeUnit.MINUTES);
            List<KeyValue> kvs = getResponse.getKvs();
            if (kvs.size() > 0) {
                kv = kvs.get(0);
            }
        } catch (Exception e) {
            logger.error("etcd delete bucketName dns record failed, key: " + key, e);
        }
        return kv;
    }

    /**
     * 将桶和区域从etcd删掉
     *
     * @param regionName 区域
     * @param bucketName 桶
     */
    public static void delete(String regionName, String bucketName) {
        if (StringUtils.isEmpty(regionName)) {
            return;
        }
        try {
            String key = getDnsRecordPrefix(bucketName);
            getClient.delete(ByteSequence.from(key.getBytes())).get(1, TimeUnit.MINUTES);
        } catch (Exception e) {
            logger.error("etcd delete bucketName dns record failed, bucketName: " + bucketName, e);
        }
    }

    public static void delete(String key) {
        try {
            getClient.delete(ByteSequence.from(key.getBytes())).get(1, TimeUnit.MINUTES);
        } catch (Exception e) {
            logger.error("etcd delete bucketName dns record failed, key: " + key, e);
        }
    }

    public static List<KeyValue> selectByPrefixWithAscend(String key) {
        List<KeyValue> keyValues = new ArrayList<>();
        for (int i = 0; i < TRY_TIMES; i++) {
            try {
                GetOption getOption = GetOption.newBuilder()
                        .withPrefix(ByteSequence.from(key.getBytes()))
                        .withSortOrder(GetOption.SortOrder.ASCEND)
                        .withLimit(1000)
                        .build();
                GetResponse getResponse = getClient.get(ByteSequence.from(key.getBytes()), getOption).get(TIMEOUT, TimeUnit.SECONDS);
                keyValues = getResponse.getKvs();
                break;
            } catch (Exception e) {
                logger.error("etcd find action by ascend failed.", e);
            }
        }
        return keyValues;
    }

    public static long getCountByPrefix(String key) {
        long count = 0;
        for (int i = 0; i < TRY_TIMES; i++) {
            try {
                GetOption getOption = GetOption.newBuilder()
                        .withPrefix(ByteSequence.from(key.getBytes()))
                        .withCountOnly(true)
                        .build();
                GetResponse getResponse = getClient.get(ByteSequence.from(key.getBytes()), getOption).get(TIMEOUT, TimeUnit.SECONDS);
                count = getResponse.getCount();
                break;
            } catch (Exception e) {
                logger.error("etcd find action by ascend failed.", e);
            }
        }
        return count;
    }

    /**
     * @param bucketName 桶名
     * @return MOSS系统DNS的记录前缀
     */
    public static String getDnsRecordPrefix(String bucketName) {
        return getCompanyPrefix().append(dnsPrefix).append("/").append(bucketName).toString();
    }

    public static String getDnsRecordPrefix(String bucketName,String dnsName) {
        String dnsPrefix = dnsName.split("\\.")[0];
        return getCompanyPrefix(dnsName).append(dnsPrefix).append("/").append(bucketName).toString();
    }

    /**
     * @return 客户自定义配置的路径前缀的strbuilder
     */
    public static StringBuilder getCompanyPrefix() {
        StringBuilder stringBuilder = new StringBuilder(CORE_DNS_DIR_PREFIX);
        String dnsSuffix = getDnsSuffix();
        String[] dnsSplit = dnsSuffix.split("\\.");
        //倒序遍历
        for (int i = dnsSplit.length - 1; i >= 0; i--) {
            stringBuilder.append(dnsSplit[i]).append("/");
        }
        logger.debug("getCompanyPrefix = {}", stringBuilder);
        return stringBuilder;
    }

    public static StringBuilder getCompanyPrefix(String dnsName) {
        StringBuilder stringBuilder = new StringBuilder(CORE_DNS_DIR_PREFIX);
        String dnsSuffix = getDnsSuffix(dnsName);
        String[] dnsSplit = dnsSuffix.split("\\.");
        //倒序遍历
        for (int i = dnsSplit.length - 1; i >= 0; i--) {
            stringBuilder.append(dnsSplit[i]).append("/");
        }
        logger.debug("getCompanyPrefix = {}", stringBuilder);
        return stringBuilder;
    }

    /**
     * 获取dns后缀
     *
     * @return 域名后缀
     */
    public static String getDnsSuffix() {
        String dnsName = pool.getCommand(REDIS_SYSINFO_INDEX).get(DNS_NAME);
        String prefix = dnsPrefix + ".";
        if (dnsName.startsWith(prefix)) {
            return StringUtils.substringAfter(dnsName, prefix);
        } else {
            return dnsName;
        }
    }

    public static String getDnsSuffix(String dnsName) {
        String prefix = dnsName.split("\\.")[0] + ".";
        return StringUtils.substringAfter(dnsName, prefix);
    }

    /**
     * @param record 记录
     * @return 返回记录的文本形式  如: {"host":"172.32.20.2","ttl":1800}
     */
    public static String getRecordStr(String record) {
        long ttl_value = getDnsTtl();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(ETCD_RECORD_HOST, record);
        jsonObject.put(ETCD_RECORD_TTL, ttl_value);
        return jsonObject.toString();
    }

    /**
     * @param regionName 区域名称
     * @return 返回指定区域的域名
     */
    public static String getRegionDns(String regionName) {
        String prefix = dnsPrefix + ".";
        return prefix + regionName + "." + getDnsSuffix();
    }

    public static String getRegionDns(String regionName,String dnsName) {
        String prefix = dnsName.split("\\.")[0] + ".";
        return prefix + regionName + "." + getDnsSuffix(dnsName);
    }

    public static long getDnsTtl(){
        String ttl_value = pool.getCommand(REDIS_SYSINFO_INDEX).get(DNS_TTL);
        long ttl;
        if(StringUtils.isEmpty(ttl_value) || "null".equals(ttl_value)){
            ttl = TTL_VALUE;
        }else {
            ttl = Long.parseLong(ttl_value);
        }
        return ttl;
    }
}

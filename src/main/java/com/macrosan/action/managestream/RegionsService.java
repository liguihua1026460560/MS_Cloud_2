package com.macrosan.action.managestream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.database.etcd.EtcdClient;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.Credential;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.message.socketmsg.MapResMsg;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.message.socketmsg.SocketSender;
import com.macrosan.message.socketmsg.StringResMsg;
import com.macrosan.message.xmlmsg.region.GetBucketLocationOutput;
import com.macrosan.message.xmlmsg.region.LocationResult;
import com.macrosan.storage.metaserver.BucketShardCache;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.msutils.SshClientUtils;
import com.macrosan.utils.policy.PolicyCheckUtils;
import com.macrosan.utils.regions.MultiRegionUtils;
import com.macrosan.utils.sts.StsCredentialSyncTask;
import io.etcd.jetcd.KeyValue;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.vertx.core.json.Json;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import reactor.core.publisher.Flux;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.macrosan.constants.ErrorNo.UNKNOWN_ERROR;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.HeartBeatChecker.syncPolicy;
import static com.macrosan.ec.server.ErasureServer.DISK_SCHEDULER;
import static com.macrosan.httpserver.MossHttpClient.LOCAL_CLUSTER_INDEX;
import static com.macrosan.httpserver.MossHttpClient.syncEthToType;
import static com.macrosan.utils.sts.StsCredentialSyncTask.CRED_TYPE_REFERENCE;


/**
 * 开启多区域后桶的相关接口
 *
 * @author chengyinfeng
 */
public class RegionsService extends BaseService {

    private static Logger logger = LogManager.getLogger(RegionsService.class.getName());

    private static RegionsService instance = null;
    protected static SocketSender sender = SocketSender.getInstance();

    private RegionsService() {
        super();
    }


    /**
     * 每一个Service都必须提供一个getInstance方法
     */
    public static RegionsService getInstance() {
        if (instance == null) {
            instance = new RegionsService();
        }
        return instance;
    }


    /**
     * 获取桶的区域信息
     *
     * @param paramMap 请求参数
     * @return xml
     */
    public ResponseMsg getBucketLocation(UnifiedMap<String, String> paramMap) {
        String userId = paramMap.get(USER_ID);
        String bucketName = paramMap.get(BUCKET_NAME);

        Map<String, String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
        if (bucketInfo.isEmpty()) {
            throw new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }

        String method = "GetBucketLocation";
        int policyResult = PolicyCheckUtils.getPolicyResult(paramMap, bucketName, userId, method);
        MsAclUtils.checkIfAnonymous(userId);

        if (!userId.equals(bucketInfo.get(BUCKET_USER_ID))) {
            throw new MsException(ErrorNo.NO_BUCKET_PERMISSION,
                    "no permission.user " + userId + " can not get " + bucketName + " location.");
        }
        String location = Optional.ofNullable(bucketInfo.get(REGION_NAME)).orElse("NULL");
        return new ResponseMsg().setData(new GetBucketLocationOutput(location))
                .addHeader(CONTENT_TYPE, "application/xml");
    }

    /**
     * 获取当前位置登录的区域信息与站点信息
     *
     * @param paramMap
     * @return
     */
    public ResponseMsg getLocation(UnifiedMap<String, String> paramMap) {
        Map<String, String> clusterInfo = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall(LOCAL_CLUSTER);
        String regionName = Optional.ofNullable(clusterInfo.get(REGION_NAME)).orElse("NULL");
        if ("NULL".equals(regionName)) {
            regionName = Optional.ofNullable(pool.getCommand(REDIS_SYSINFO_INDEX).hget(MULTI_REGION_LOCAL_REGION, REGION_NAME)).orElse("NULL");
        }
        String clusterName = Optional.ofNullable(clusterInfo.get(CLUSTER_NAME)).orElse("NULL");
        LocationResult locationResult = new LocationResult();
        locationResult.setRegionName(regionName);
        locationResult.setClusterName(clusterName);
        return new ResponseMsg().setData(locationResult)
                .addHeader(CONTENT_TYPE, "application/xml");
    }

    /**
     * 多区域
     *
     * @param paramMap 请求参数
     * @return 处理结果
     */
    public ResponseMsg multiRegionInfo(UnifiedMap<String, String> paramMap) {
        String param = paramMap.get("body");
        JSONObject jsonParam = JSON.parseObject(param);
        String type = jsonParam.getString(MULTI_REGION_REQ_TYPE);
        if (!MSG_TYPE_REGIONS_STATISTICS_SYNC.equals(type) && !MSG_TYPE_REGIONS_STATISTICS_MONTH.equals(type)) {
            logger.info("body: " + param);
        }
        ResponseMsg msgs = new ResponseMsg();
        boolean rtnFlag = false;
        switch (type) {
            case MULTI_REGION_UPDATE_DNS_IP:
                //发socket通知cloud,把数据放进去
                SocketReqMsg dnsSocket = new SocketReqMsg(MULTI_REGION_UPDATE_DNS_IP, 0)
                        .put(AIP, jsonParam.getString(AIP))
                        .put(REGION_DNS_IPS, jsonParam.getString(REGION_DNS_IPS))
                        .put(CLUSTER_NAMES, jsonParam.getString(CLUSTER_NAMES))
                        .put(REGION_CN_NAME, jsonParam.getString(REGION_CN_NAME))
                        .put(REGION_NAME, jsonParam.getString(REGION_NAME));
                StringResMsg msg1 = sender.sendAndGetResponse(dnsSocket, StringResMsg.class, true);
                if (msg1.getCode() != ErrorNo.SUCCESS_STATUS) {
                    throw new MsException(UNKNOWN_ERROR, "update other dns ip error.");
                }
                rtnFlag = true;
                break;
            case MULTI_SET_BUCKET_SYNC:
                String bucket1 = jsonParam.getString("bucket");
                String syncType = jsonParam.getString("syncType");
                String compressType = jsonParam.getString("compressType");
                String sourceSign = jsonParam.getString("sourceSign");
                String ak0 = jsonParam.getString("ak");
                String sk0 = jsonParam.getString("sk");
                String syncIndex = jsonParam.getString("sync-index");
                String archiveIndex = jsonParam.getString("archive-index");
                Map<String, Set<Integer>> archiveMap = StringUtils.isBlank(archiveIndex) ? null : JSON.parseObject(archiveIndex, new TypeReference<Map<String, Set<Integer>>>() {
                });
                Map<String, Set<Integer>> syncMap = StringUtils.isBlank(syncIndex) ? null : JSON.parseObject(syncIndex, new TypeReference<Map<String, Set<Integer>>>() {
                });
                if (pool.getCommand(REDIS_BUCKETINFO_INDEX).exists(bucket1) != 1) {
                    throw new MsException(UNKNOWN_ERROR, "set bucket sync switch error. bucket is not exist: " + bucket1);
                }
                JSONObject cacheJson1 = new JSONObject();
                long currentTime = System.currentTimeMillis();
                cacheJson1.put("syncTime", currentTime);
                cacheJson1.put("syncType", syncType);
                cacheJson1.put("ak", ak0);
                cacheJson1.put("sk", sk0);
                cacheJson1.put("sync-index", syncMap);
                cacheJson1.put("archive-index", archiveMap);
                if ("on".equals(syncType)) {
                    if (jsonParam.containsKey("bucketInfo")) {
                        final String bucketStr = jsonParam.getString("bucketInfo");
                        Map<String, String> bucketInfo = JSON.parseObject(bucketStr, new TypeReference<Map<String, String>>() {
                        });
                        for (Map.Entry<String, String> entry : bucketInfo.entrySet()) {
                            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, entry.getKey(), entry.getValue());
                        }
                        cacheJson1.put("bucketInfo", bucketInfo);
                    }
                    if (jsonParam.containsKey("policyMap")) {
                        final String policyStr = jsonParam.getString("policyMap");
                        Map<String, String> policyMap = JSON.parseObject(policyStr, new TypeReference<Map<String, String>>() {
                        });
                        policyMap.forEach((k, v) -> pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).hset(bucket1 + "_policys", k, v));
                        cacheJson1.put("policyMap", policyMap);
                    }
                    if (jsonParam.containsKey("lifeRule") && "0".equals(syncPolicy)) {
                        final String lifeRule = jsonParam.getString("lifeRule");
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset("bucket_lifecycle_rules", bucket1, lifeRule);
                        cacheJson1.put("lifeRule", lifeRule);
                    }
                    if (jsonParam.containsKey("aclMap")) {
                        final String aclStr = jsonParam.getString("aclMap");
                        Map<String, Set<String>> aclMap = JSON.parseObject(aclStr, new TypeReference<Map<String, Set<String>>>() {
                        });
                        aclMap.forEach((k, set) -> {
                            set.forEach(v -> pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).sadd(k, v));
                        });
                        cacheJson1.put("aclMap", aclMap);
                    }
                    if (jsonParam.containsKey("inventoryMap")) {
                        final String inventoryStr = jsonParam.getString("inventoryMap");
                        Map<String, String> inventoryMap = JSON.parseObject(inventoryStr, new TypeReference<Map<String, String>>() {
                        });
                        inventoryMap.forEach((k, v) -> {
                            pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).hset(bucket1 + INVENTORY_SUFFIX, k, v);
                            pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).zadd(bucket1 + INVENTORY_SUFFIX_ID, INVENTORY_SCORE, k);
                        });
                        cacheJson1.put("inventoryMap", inventoryMap);
                    }
                    if (jsonParam.containsKey("mda") && "on".equals(jsonParam.getString("mda"))) {
                        // 同步元数据分析开关
                        String strategy = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket1, "storage_strategy");
                        JSONArray esArray = JSONArray.parseArray(Optional.ofNullable(pool.getCommand(REDIS_POOL_INDEX).hget(strategy, "es")).orElse("[]"));
                        if (esArray.size() == 0) {
                            logger.error("This bucket does not support metadata switches: " + bucket1);
                        } else {
                            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd(ES_BUCKET_SET, bucket1);
                            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, "mda", "on");
                        }
                    }

                    if (jsonParam.containsKey("squash")) {
                        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, "squash", jsonParam.getString("squash"));
                    }
                    if (jsonParam.containsKey("anonuid")) {
                        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, "anonuid", jsonParam.getString("anonuid"));
                    } else {
                        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucket1, "anonuid");
                    }
                    if (jsonParam.containsKey("anongid")) {
                        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, "anongid", jsonParam.getString("anongid"));
                    } else {
                        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucket1, "anongid");
                    }
                    if (jsonParam.containsKey("mountPoint")) {
                        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, "mountPoint", jsonParam.getString("mountPoint"));
                    }
                    if (jsonParam.containsKey("nfsAcl")) {
                        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, "nfsAcl", jsonParam.getString("nfsAcl"));
                    }
                    if (jsonParam.containsKey("nfs")) {
                        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, "nfs", jsonParam.getString("nfs"));
                    }
                    if (jsonParam.containsKey("cifs")) {
                        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, "cifs", jsonParam.getString("cifs"));
                    }
                    if (jsonParam.containsKey("caseSensitive")) {
                        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, "caseSensitive", jsonParam.getString("caseSensitive"));
                    }
                    if (jsonParam.containsKey("guest")) {
                        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, "guest", jsonParam.getString("guest"));
                    }
                    if (jsonParam.containsKey("cifsAcl")) {
                        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, "cifsAcl", jsonParam.getString("cifsAcl"));
                    }
                    if (jsonParam.containsKey("ftp")) {
                        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, "ftp", jsonParam.getString("ftp"));
                    }
                    if (jsonParam.containsKey("ftpAcl")) {
                        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, "ftpAcl", jsonParam.getString("ftpAcl"));
                    }
                    if (jsonParam.containsKey("ftp_anonymous")) {
                        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, "ftp_anonymous", jsonParam.getString("ftp_anonymous"));
                    }
                    if (jsonParam.containsKey("nfsIpWhitelist")) {
                        String nfsIpWhitelist = jsonParam.getString("nfsIpWhitelist");
                        List<String> valueList = JSON.parseObject(nfsIpWhitelist, new TypeReference<List<String>>() {
                        });
                        for (Object value : valueList) {
                            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).lpush(bucket1 + "_nfsIpWhitelist", String.valueOf(value));
                        }
                    }
                    for (FSPerformanceService.Instance_Type value : FSPerformanceService.Instance_Type.values()) {
                        String instance = value.name();
                        if (jsonParam.containsKey(instance + "-" + THROUGHPUT_QUOTA)) {
                            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, instance + "-" + THROUGHPUT_QUOTA, jsonParam.getString(instance + "-" + THROUGHPUT_QUOTA));
                        }
                        if (jsonParam.containsKey(instance + "-" + BAND_WIDTH_QUOTA)) {
                            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, instance + "-" + BAND_WIDTH_QUOTA, jsonParam.getString(instance + "-" + BAND_WIDTH_QUOTA));
                        }
                    }
                    if (jsonParam.containsKey("nfsQuotaMap")) {
                        final String nfsQuotaStr = jsonParam.getString("nfsQuotaMap");
                        Map<String, String> nfsQuotaMap = JSON.parseObject(nfsQuotaStr, new TypeReference<Map<String, String>>() {
                        });
                        nfsQuotaMap.forEach((k, v) -> {
                            pool.getShortMasterCommand(REDIS_FS_QUOTA_INFO_INDEX).hset(bucket1 + "_quota", k, v);
                        });
                        cacheJson1.put("nfsQuotaMap", nfsQuotaMap);
                    }
                    pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, "data-synchronization-switch", "on");
                } else if ("suspend".equals(syncType)) {
                    pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, DATA_SYNC_SWITCH, "suspend");
                } else if ("closed".equals(syncType)) {
                    pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, DATA_SYNC_SWITCH, "closed");
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).srem("need_sync_buckets", bucket1);
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hdel("bucket_backup_rules", bucket1);
                    pool.getShortMasterCommand(REDIS_TASKINFO_INDEX).del(bucket1 + "_backup_record");
                    pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucket1, "part_bucket_sync_finished");
                    pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hdel(bucket1, "obj_bucket_sync_finished");
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd("closed_bucket", bucket1);
                }
                if (StringUtils.isNotBlank(compressType)) {
                    pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, SYNC_COMPRESS, compressType);
                }
                if (StringUtils.isNotBlank(sourceSign)) {
                    pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket1, DELETE_SOURCE_SIGN, sourceSign);
                }
                setBucketStatus(bucket1, ak0, sk0, syncMap, archiveMap);
                String ctime = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket1, "ctime");
                cacheJson1.put("compressType", compressType);
                cacheJson1.put("sourceSign", sourceSign);
                cacheJson1.put("sourceType", "");
                cacheJson1.put("ctime", ctime);
                EtcdClient.put("/bucketSync/" + bucket1, cacheJson1.toString());
                logger.info("set bucket sync server success: {}", bucket1);
                break;
            case MULTI_UPDATE_SERVER_AK_SK:
                final String bucket = jsonParam.getString("bucket");
                final String ak = jsonParam.getString("Ak");
                final String sk = jsonParam.getString("Sk");
                if (pool.getCommand(REDIS_BUCKETINFO_INDEX).exists(bucket) != 1) {
                    throw new MsException(UNKNOWN_ERROR, "update bucket ak sk error, bucket is not exist.");
                }
                pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket, "server_ak", ak);
                pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket, "server_sk", sk);
                rtnFlag = true;
                break;
            case MULTI_CHANGE_SYNC_SSL:
                String ssl = jsonParam.getString("ssl");
                long currentTimeMillis = System.currentTimeMillis();
                JSONObject cacheJson = new JSONObject();
                cacheJson.put("ssl", ssl);
                cacheJson.put("syncTime", currentTimeMillis);
                EtcdClient.put("/ssl/syncMethod", cacheJson.toString());
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).set("sync_method", ssl);
                String command = String.format("insert into moss_config (moss_key, moss_value) values ('sslSyncType', \'%s\') on DUPLICATE KEY update moss_value = \'%s\';", ssl, ssl);
                String host = ServerConfig.getInstance().getMasterVip1();
                String execCommand = String.format("mysql -uroot -p123456 -h%s moss_web -e \"%s\"",
                        host,
                        command);
                logger.info("exec: {}", execCommand);
                Tuple2<String, String> execTuple2 = SshClientUtils.exec(execCommand, true);
                if (StringUtils.isNotBlank(execTuple2.var2)) {
                    throw new RuntimeException("exec MySQL command fail. " + execTuple2.var2);
                }
                rtnFlag = true;
                break;
            case MULTI_CHECK_SYNC_SSL:
                Long change_sync_ssl = pool.getCommand(REDIS_SYSINFO_INDEX).exists("change_sync_ssl");
                if (change_sync_ssl != 0) {
                    throw new MsException(UNKNOWN_ERROR, "Exists ssl sync task.");
                }
                rtnFlag = true;
                break;
            case MULTI_REGION_MODIFY_REMOTE_IPS:
                String modifySite = jsonParam.getString("siteName");
                String remoteIps = jsonParam.getString("ipList");
                Integer remoteIndex = jsonParam.getInteger("index");
                String syncEth4 = jsonParam.getString("syncEth");
                String siteIps2 = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_IPS);
                String masterName = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
                String[] ipSplit2 = siteIps2.split(";");
                ipSplit2[remoteIndex] = remoteIps;
                StringBuilder strB = new StringBuilder();
                for (String ips : ipSplit2) {
                    if (strB.length() > 0) {
                        strB.append(";").append(ips);
                    } else {
                        strB.append(ips);
                    }
                }

                // 修改local_cluster
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, CLUSTER_IPS, strB.toString());
                // 修改other_cluster
                Set<String> clusterSet = pool.getCommand(REDIS_SYSINFO_INDEX).smembers(OTHER_CLUSTERS);
                for (String site : clusterSet) {
                    JSONObject jsonObject = JSONObject.parseObject(site);
                    String siteName = jsonObject.getString(CLUSTER_NAME);
                    if (siteName.equals(modifySite)) {
                        // 修改master_cluster
                        if (masterName.equals(siteName)) {
                            pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, CLUSTER_IPS, remoteIps);
                        }
                        jsonObject.put(CLUSTER_IPS, remoteIps);
                        jsonObject.put("sync_eth", syncEth4);
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).srem(OTHER_CLUSTERS, site);
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd(OTHER_CLUSTERS, jsonObject.toString());
                        break;
                    }
                }
                String host1 = ServerConfig.getInstance().getMasterVip1();
                String sql = String.format("update site_info set sync_eth='%s' where site_name='%s';", syncEth4, modifySite);
                String sqlCommand = String.format("mysql -uroot -p123456 -h%s moss_web -e \"%s\"", host1, sql);
                logger.info("exec: {}", sqlCommand);
                Tuple2<String, String> sqlTuple2 = SshClientUtils.exec(sqlCommand, true);
                if (StringUtils.isNotBlank(sqlTuple2.var2)) {
                    throw new RuntimeException("exec MySQL command fail. " + sqlTuple2.var2);
                }

                msgs.setData("1");
                rtnFlag = true;
                break;
            case MULTI_REGION_TYPE_LOCK:
                int res = MultiRegionUtils.getSingleLock(jsonParam.getInteger(MULTI_REGION_DB), jsonParam.getString(MULTI_REGION_KEY));
                if (res != ErrorNo.SUCCESS_STATUS) {
                    throw new MsException(UNKNOWN_ERROR, "get redis lock error.");
                }
                break;
            case MULTI_REGION_DNS_INFO:
                JSONObject json = new JSONObject();
                Map<String, String> dnsIps = pool.getCommand(REDIS_SYSINFO_INDEX).hgetall(DNS_IP_KEY);
                String dnsName = pool.getCommand(REDIS_SYSINFO_INDEX).get(DNS_NAME);
                String areaName = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MULTI_REGION_LOCAL_REGION, REGION_NAME);
                String areaCname = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MULTI_REGION_LOCAL_REGION, REGION_CN_NAME);
                String masterNode = pool.getCommand(REDIS_SYSINFO_INDEX).get("masterDnsuuid");
                String masterFrontIps = "";
                masterFrontIps = pool.getCommand(REDIS_NODEINFO_INDEX).hget(masterNode, "business_eth1");
                String business_eth2 = pool.getCommand(REDIS_NODEINFO_INDEX).hget(masterNode, "business_eth2");
                if (!"0".equals(business_eth2)) {
                    masterFrontIps += ("," + business_eth2);
                }
                // 获取当前区域的所有站点信息
                // todo 若存在一个区域下部署多个不相关的站点时，无法获取到该区域下所有的站点信息
                List<String> clusterNameList = new ArrayList<>();
                clusterNameList.add(ServerConfig.getInstance().getSite());
                Set<String> otherClusters = pool.getCommand(REDIS_SYSINFO_INDEX).smembers(OTHER_CLUSTERS);
                for (String otherCluster : otherClusters) {
                    JSONObject cluster = JSON.parseObject(otherCluster);
                    clusterNameList.add(cluster.getString(CLUSTER_NAME));
                }

                SshClientUtils.exec("rm -rf /moss/backup");

                Set<String> smembers = pool.getCommand(REDIS_SYSINFO_INDEX).smembers(OTHER_REGIONS);
                if (!smembers.isEmpty()) {
                    json.put("other_regions", JSON.toJSONString(smembers));
                } else {
                    json.put("other_regions", "");
                }

                String dnsIpStr = JSON.toJSONString(dnsIps);
                json.put("dns_ips", dnsIpStr);
                json.put("dns_name", dnsName);
                json.put("area_name", areaName);
                json.put("cname", areaCname);
                json.put("aip", config.getAccessIp());
                json.put("cluster_names", JSON.toJSONString(clusterNameList));
                json.put("masterFrontIps", masterFrontIps);
                logger.info("ALL DNS IPS IS:" + json.toString());
                msgs.setData(json.toString());
                rtnFlag = true;
                break;
            case MULTI_REGION_CHECK_ENVIRONMENT:
                String dnsIpList = jsonParam.getString("dnsIps");
                JSONObject regionJson = new JSONObject();
                if (!isReachable(dnsIpList)) {
                    regionJson.put("code", "2");
                } else {
                    // 获取当前站点的存储策略信息
                    SocketReqMsg licenseSocket = new SocketReqMsg(MULTI_REGION_CHECK_LICENSE, 0);
                    StringResMsg checkLicenseRes = sender.sendAndGetResponse(licenseSocket, StringResMsg.class, true);
                    logger.info("checkLicenseRes: {}", checkLicenseRes.getCode());
                    if (checkLicenseRes.getCode() != ErrorNo.SUCCESS_STATUS) {
                        regionJson.put("code", "-1");
                    } else {
                        List<String> strategyKeys = pool.getCommand(REDIS_POOL_INDEX).keys("strategy_*");
                        Map<String, Map<String, String>> strategyMap = new HashMap<>();

                        String defaultStrategy = null;
                        for (String key : strategyKeys) {
                            Map<String, String> strategy = pool.getCommand(REDIS_POOL_INDEX).hgetall(key);
                            boolean isDefault = Boolean.parseBoolean(strategy.getOrDefault("is_default", "false"));
                            if (isDefault) {
                                defaultStrategy = key;
                            }
                            strategyMap.put(key, strategy);
                        }

                        if (!strategyMap.isEmpty()) {
                            regionJson.put("strategyMap", strategyMap);
                        }

                        if (defaultStrategy != null) {
                            regionJson.put("defaultStrategy", defaultStrategy);
                        }

                        SocketReqMsg envSocket = new SocketReqMsg(MULTI_REGION_CHECK_ENVIRONMENT, 0);
                        StringResMsg envMsg = sender.sendAndGetResponse(envSocket, StringResMsg.class, true);
                        int codes = envMsg.getCode();
                        regionJson.put("code", codes + "");
                    }
                }
                msgs.setData(regionJson.toString());
                rtnFlag = true;
                break;
            case MULTI_REGION_ZERO_TO_ONE:
                //发socket通知cloud,找到数据并返回 没写完
                SocketReqMsg backSocket = new SocketReqMsg("findDataAndBack", 0);
                StringResMsg backMsg = sender.sendAndGetResponse(backSocket, StringResMsg.class, true);
                msgs.setData(backMsg.getData());
                rtnFlag = true;
                break;
            case MULTI_REGION_ONE_TO_ZERO:
                String accounts = jsonParam.getString("account");
                String buckets = jsonParam.getString("bucket");
                //发socket通知cloud,把数据放进去
                SocketReqMsg equalSocket = new SocketReqMsg("putDataInRedis", 0)
                        .put("account", accounts)
                        .put("bucket", buckets);
                StringResMsg equalMsg = sender.sendAndGetResponse(equalSocket, StringResMsg.class, true);
                rtnFlag = true;
                break;
            case MULTI_REGION_INFO:
                String regionInfo = jsonParam.getString("regionVo");
                String defaultRegion = jsonParam.getString("default_region");
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd(OTHER_REGIONS, regionInfo);

                //不直接写是考虑如果已经有默认区域了，就按原来的来
                String localDefaultRegion = pool.getCommand(REDIS_SYSINFO_INDEX).get(MULTI_REGION_DEFAULT_REGION);
                if (StringUtils.isEmpty(localDefaultRegion)) {
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).set(MULTI_REGION_DEFAULT_REGION, defaultRegion);
                } else {
                    defaultRegion = localDefaultRegion;
                }
                //发socket通知cloud，让cloud通知web将区域信息插入数据库
                SocketReqMsg rgnSocket = new SocketReqMsg(MULTI_REGION_ADD_MYSQL, 0)
                        .put("regionVo", regionInfo);
                StringResMsg rgnMsg = sender.sendAndGetResponse(rgnSocket, StringResMsg.class, true);
                if (rgnMsg.getCode() != ErrorNo.SUCCESS_STATUS || infoIam() != ErrorNo.SUCCESS_STATUS) {
                    throw new MsException(UNKNOWN_ERROR, "set region info error.");
                }
                msgs.setData(defaultRegion);
                rtnFlag = true;
                break;
            case "local_inform_iam":
                logger.info("local_inform_iam in MS_CLOUD");
                return new ResponseMsg().setData(infoIam() + "");
            case "changeTopLevel":
                String topLevel = jsonParam.getString("topLevel");
                //发socket通知cloud，让cloud通知web将修改一级域名
                SocketReqMsg dmSocket = new SocketReqMsg("changeTopLevel", 0)
                        .put("topLevel", topLevel);
                StringResMsg dmMsg = sender.sendAndGetResponse(dmSocket, StringResMsg.class, true);
                msgs.setData(dmMsg.getCode() + "");
                rtnFlag = true;
                break;
            case "delRedisRegionEnv":
                //通知cloud,让web删掉区域信息
                SocketReqMsg delRegionSocket = new SocketReqMsg("delMysqlEnv", 0)
                        .put("srcRegionName", jsonParam.getString("srcRegionName"))
                        .put("desRegionName", jsonParam.getString("desRegionName"))
                        .put("type", "0");
                StringResMsg delRegionMsg = sender.sendAndGetResponse(delRegionSocket, StringResMsg.class, true);
                msgs.setData(delRegionMsg.getCode() + "");
                rtnFlag = true;
                break;
            case "modifyOtherDomain":
                SocketReqMsg mdDomainSocket = new SocketReqMsg("modifyOtherDomain", 0)
                        .put("topLevel", jsonParam.getString("newDomain"))
                        .put("source", jsonParam.getString("source"));
                StringResMsg modRegionMsg = sender.sendAndGetResponse(mdDomainSocket, StringResMsg.class, true);
                logger.info(modRegionMsg.getCode());
                break;
//            case MSG_TYPE_REGIONS_STATISTICS_SYNC:
//                QuotaRecorder.setRegionStorageData(jsonParam.getString("storage"));
//                StatisticsRecorder.setRegionStatisticsData(jsonParam.getString("statistics"));
//                break;
//		    case MSG_TYPE_REGIONS_STATISTICS_MONTH:
//                StatisticsRecorder.checkMonthStatisticsData(jsonParam.getString("monthRecords"));
//                break;
            case "checkRemoteSite":
                String dnsIpList2 = jsonParam.getString("dnsIps");
                if (!isReachable(dnsIpList2)) {
                    msgs.setData("2");
                } else {
                    SocketReqMsg envSocket = new SocketReqMsg(MULTI_REGION_CHECK_ENVIRONMENT, 0);
                    StringResMsg envMsg = sender.sendAndGetResponse(envSocket, StringResMsg.class, true);
                    int codes = envMsg.getCode();
                    msgs.setData(codes + "");
                }
                rtnFlag = true;
                break;
            case "recoveryData":
                BucketShardCache.listenSyncing();
                SocketReqMsg socket = new SocketReqMsg("recoveryData", 0)
                        .put("md5Map", jsonParam.getString("md5Map"))
                        .put("deployType", jsonParam.containsKey("deployType") ? jsonParam.getString("deployType") : "")
                        .put("sync3dc", jsonParam.containsKey("sync3dc") ? jsonParam.getString("sync3dc") : "");

                StringResMsg resMsg = sender.sendAndGetResponse(socket, StringResMsg.class, true);
                if (resMsg.getCode() != ErrorNo.SUCCESS_STATUS) {
                    throw new MsException(UNKNOWN_ERROR, "recovery data error.");
                }
                break;
            case "listenBucketSync":
                BucketShardCache.listenSyncing();
                break;
            case "syncSts"://发至同节点前端包处理
                StsCredentialSyncTask.start(jsonParam.getString("remoteIp"));
                break;
            case "getSyncSTSRes":
                if ("1".equals(pool.getCommand(REDIS_SYSINFO_INDEX).get("syncSTSRes"))) {
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del("syncSTSRes");
                    msgs.setData("1");
                } else {
                    msgs.setData("2");
                }
                rtnFlag = true;
                break;
            case "syncCredential":
                String credList = jsonParam.getString("cred");//考虑优化为传输包含1000个凭证的集合
                if (StringUtils.isEmpty(credList)) {
                    msgs.setData("0");
                } else {
                    List<Credential> list = Json.decodeValue(credList, CRED_TYPE_REFERENCE);
                    AtomicBoolean existFalse = new AtomicBoolean(false);
                    boolean result = Flux.fromStream(list.stream())
                            .publishOn(DISK_SCHEDULER)
                            .flatMap(credential -> {
//                                Credential credential = Json.decodeValue(cred, Credential.class);
                                return StsCredentialSyncTask.syncSTSToken(credential);
                            })
                            .doOnNext(b -> {
                                if (!b) {
                                    existFalse.compareAndSet(false, true);
                                }
                            })
                            .collectList()
                            .map(l -> {
                                return existFalse.get();
                            }).block();

                    if (!result) {
                        msgs.setData("1");
                    } else {
                        msgs.setData("0");
                    }
                }
                rtnFlag = true;
                break;
            case "getRecoveryRes":
                String recoveryRes = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get("recoveryRes");
                logger.debug("recoveryRes  " + recoveryRes);
                if (!"1".equals(recoveryRes)) {
                    msgs.setData("2");
                    if ("4".equals(recoveryRes)) {
                        msgs.setData("1");
                    }
                    rtnFlag = true;
                } else {
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del("recoveryRes");
                }

                break;
            case "getRecoveryDataRes":
                String recoveryIamRes = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get("recoveryIamRes");
                String recoveryDataRes = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get("recoveryDataRes");
                logger.info(recoveryIamRes + "-------" + recoveryDataRes);
                if ("1".equals(recoveryDataRes) && "1".equals(recoveryIamRes)) {
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del("recoveryIamRes");
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del("recoveryDataRes");
                    msgs.setData("1");
                } else {
                    msgs.setData("2");
                }

                rtnFlag = true;
                break;
            case "updateBucketIndex":
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del("updateBucketIndex");
                try {
                    new Thread(() -> {
                        // 2. 获取 DB size
                        Long dbSizeSum = pool.getCommand(REDIS_BUCKETINFO_INDEX).dbsize();
                        logger.info("bucket db size = {}", dbSizeSum);

                        // 3. 扫描配置
                        ScanArgs scanArgs = ScanArgs.Builder.limit(10000);
                        String cursor = ScanCursor.INITIAL.getCursor();
                        boolean continueScan = true;

                        // 4. 线程池与计数器
                        ExecutorService executorService = Executors.newFixedThreadPool(16);
                        Set<String> bucketSet = ConcurrentHashMap.newKeySet();
                        Map<String, Set<Integer>> syncMapNew = new HashMap<>();
                        Map<String, Set<Integer>> archiveMapNew = new HashMap<>();
                        String indexInfo = jsonParam.getString("indexInfo");
                        Map<String, int[]> pattern = new HashMap<>();
                        Map<String, int[]> pattern2 = new HashMap<>();
                        if ("0".equals(indexInfo)) {
                            if (LOCAL_CLUSTER_INDEX == 0) {
                                pattern.put("1", new int[]{0, 2});
                                pattern.put("2", new int[]{0, 1});
                            } else {
                                pattern.put("0", new int[]{1, 2});
                                pattern.put("1", new int[]{0, 2});
                            }
                        } else {
                            pattern.put("0", new int[]{1, 2});
                            pattern.put("1", new int[]{0, 2});
                            pattern.put("2", new int[]{0, 1});
                            pattern2.put("0", new int[]{1, 2});
                            pattern2.put("1", new int[]{0, 2});
                            pattern2.put("2", new int[]{0, 1});
                        }

                        for (Map.Entry<String, int[]> entry : pattern.entrySet()) {
                            syncMapNew.put(entry.getKey(), Arrays.stream(entry.getValue()).boxed().collect(Collectors.toSet()));
                        }
                        for (Map.Entry<String, int[]> entry : pattern2.entrySet()) {
                            archiveMapNew.put(entry.getKey(), Arrays.stream(entry.getValue()).boxed().collect(Collectors.toSet()));
                        }
                        final String syncIndexNew = JSON.toJSONString(syncMapNew);
                        final String archiveIndexNew = JSON.toJSONString(archiveMapNew);
                        logger.info("syncMap: " + syncIndexNew);
                        logger.info("archiveMap: " + archiveIndexNew);
                        // 5. 循环 SCAN
                        while (continueScan) {
                            KeyScanCursor<String> scanResult = pool.getCommand(REDIS_BUCKETINFO_INDEX).scan(ScanCursor.of(cursor), scanArgs);
                            bucketSet.addAll(scanResult.getKeys());
                            cursor = scanResult.getCursor();
                            if (scanResult.isFinished()) {
                                continueScan = false;
                                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).set("updateBucketIndex", "1");
                            }

                            for (String buc : bucketSet) {
                                executorService.submit(() -> {
                                    try {
                                        if (pool.getCommand(REDIS_BUCKETINFO_INDEX).exists(buc) > 0) {
                                            String type1 = pool.getCommand(REDIS_BUCKETINFO_INDEX).type(buc);
                                            if ("hash".equalsIgnoreCase(type1)) {
                                                final Map<String, String> metal = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(buc);
                                                final String orDefault = metal.getOrDefault(DATA_SYNC_SWITCH, "");
                                                if ("on".equals(orDefault) || "suspend".equals(orDefault)) {
                                                    // 假设 syncMap 已定义并生成
                                                    if ("0".equals(indexInfo)) {
                                                        // 3dc
                                                        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(buc, SYNC_INDEX, syncIndexNew);
                                                        pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(buc, ARCHIVE_INDEX, new HashMap<>().toString());
                                                    } else {
                                                        // 3复制
                                                        final String indexStr0 = metal.getOrDefault(SYNC_INDEX, "");
                                                        final String indexStr1 = metal.getOrDefault(ARCHIVE_INDEX, "");
                                                        if (SYNC_BUCKET_INDEX.equals(indexStr0)) {
                                                            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(buc, SYNC_INDEX, syncIndexNew);
                                                        } else if (ARCHIVE_BUCKET_INDEX.equals(indexStr1)) {
                                                            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(buc, ARCHIVE_INDEX, archiveIndexNew);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    } catch (Exception e) {
                                        logger.error("Error handling bucket {}", buc, e);
                                    }
                                });
                            }
                        }
                        // 清理资源
                        executorService.shutdown();
                        logger.info("All buckets processed.");
                    }).start();
                } catch (Exception e) {
                    logger.error("updateBucketIndex error", e);
                }
                rtnFlag = true;
                break;
            case "getBucketIndexRes":
                String getBucketIndexRes = pool.getCommand(REDIS_SYSINFO_INDEX).get("updateBucketIndex");
                logger.info("getBucketIndexRes ------- " + getBucketIndexRes);
                if ("1".equals(getBucketIndexRes)) {
                    msgs.setData("1");
                } else {
                    msgs.setData("2");
                }
                rtnFlag = true;
                break;
            case ACTIVE_GET_SITE_INFO:
                String token = jsonParam.getString("token");
                String deployType = jsonParam.getString("deployType");
                String ethType = jsonParam.getString("ethType");
                String remoteIp = jsonParam.getString("remoteIp");
                SocketReqMsg msg = new SocketReqMsg("checkToken", 0);
                msg.put("token", token);
                msg.put("deployType", deployType);
                msg.put("remoteIp", remoteIp);
                msg.put("ethType", ethType);
                StringResMsg checkTokenRes = sender.sendAndGetResponse(msg, StringResMsg.class, true);
                logger.info("checkTokenRes: {}", checkTokenRes.getCode());
                if (checkTokenRes.getCode() != ErrorNo.SUCCESS_STATUS) {
                    return msgs.setData(checkTokenRes.getCode() + "");
                }
                String syncEth = checkTokenRes.getData();
                String eth = syncEthToType(syncEth, ethType);
                JSONObject siteInfo = new JSONObject();
                StringBuilder syncIps = new StringBuilder();
                String masterSyncIp = "";
                String masterUUID = pool.getCommand(REDIS_SYSINFO_INDEX).get("masterDnsuuid");
                for (String node : pool.getCommand(REDIS_NODEINFO_INDEX).keys("*")) {
                    Map<String, String> nodeMap = pool.getCommand(REDIS_NODEINFO_INDEX).hgetall(node);
                    String syncIp = nodeMap.get(eth);
                    syncIps.append(syncIp).append(",");
                    if (node.equals(masterUUID)) {
                        masterSyncIp = syncIp;
                    }
                }
                String ipList = syncIps.substring(0, syncIps.length() - 1);
                String domainName = pool.getCommand(REDIS_SYSINFO_INDEX).get(DNS_NAME);
                String regionName = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MULTI_REGION_LOCAL_REGION, REGION_NAME);
                String siteName = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
                final Long exists = pool.getCommand(REDIS_SYSINFO_INDEX).exists(MASTER_CLUSTER);

                // 获取当前站点的存储策略信息
                List<String> strategyKeys = pool.getCommand(REDIS_POOL_INDEX).keys("strategy_*");
                List<String> storagePoolKeys = pool.getCommand(REDIS_POOL_INDEX).keys("storage_*");
                Map<String, Map<String, String>> strategyMap = new HashMap<>();
                Map<String, Map<String, String>> poolMap = new HashMap<>();

                String defaultStrategy = null;
                for (String key : strategyKeys) {
                    Map<String, String> strategy = pool.getCommand(REDIS_POOL_INDEX).hgetall(key);
                    boolean isDefault = Boolean.parseBoolean(strategy.getOrDefault("is_default", "false"));
                    if (isDefault) {
                        defaultStrategy = key;
                    }
                    strategyMap.put(key, strategy);
                }

                for (String key : storagePoolKeys) {
                    Map<String, String> poolInfo = pool.getCommand(REDIS_POOL_INDEX).hgetall(key);
                    poolMap.put(key, poolInfo);
                }

                if (!strategyMap.isEmpty()) {
                    siteInfo.put("strategyMap", strategyMap);
                }

                if (!poolMap.isEmpty()) {
                    siteInfo.put("poolMap", poolMap);
                }

                if (defaultStrategy != null) {
                    siteInfo.put("defaultStrategy", defaultStrategy);
                }

                // 获取检索池信息
                List<String> poolKeys = pool.getCommand(REDIS_POOL_INDEX).keys("Pool_*");
                poolKeys.forEach(poolKey -> {
                    Map<String, String> poolInfo = pool.getCommand(REDIS_POOL_INDEX).hgetall(poolKey);
                    if ("es".equals(poolInfo.getOrDefault("role", ""))) {
                        poolMap.put("Pool_es", poolInfo);
                    }
                });

                SshClientUtils.exec("rm -rf /moss/backup");
                siteInfo.put("deployCluster", exists == 0 ? "0" : "1");
                siteInfo.put("backIps", ipList);
                siteInfo.put("domainName", domainName);
                siteInfo.put("regionName", regionName);
                siteInfo.put("siteName", siteName);
                siteInfo.put("vip", config.getMasterVip1());
                siteInfo.put("aip", config.getAccessIp());
                siteInfo.put("masterSyncIp", masterSyncIp);
                siteInfo.put("syncEth", syncEth);
                logger.info("Get local site info is: " + siteInfo.toString());
                msgs.setData(siteInfo.toString());
                rtnFlag = true;
                break;
            case ACTIVE_GET_ADD_SITE_INFO:
                String siteToken = jsonParam.getString("token");
                String remoteIp1 = jsonParam.getString("remoteIp");
                String ethType3 = jsonParam.getString("ethType");
                SocketReqMsg cMsg = new SocketReqMsg("checkToken", 0);
                cMsg.put("token", siteToken);
                cMsg.put("remoteIp", remoteIp1);
                cMsg.put("ethType", ethType3);
                StringResMsg checkRes = sender.sendAndGetResponse(cMsg, StringResMsg.class, true);
                if (checkRes.getCode() != ErrorNo.SUCCESS_STATUS) {
                    return new ResponseMsg().setData("0");
                }
                String syncEth2 = checkRes.getData();
                JSONObject sitePartInfo = new JSONObject();

                String region = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MULTI_REGION_LOCAL_REGION, REGION_NAME);
                String localName = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
                final Long masterFlag = pool.getCommand(REDIS_SYSINFO_INDEX).exists(MASTER_CLUSTER);

                sitePartInfo.put("deployCluster", masterFlag == 0 ? "0" : "1");
                sitePartInfo.put("regionName", region);
                sitePartInfo.put("siteName", localName);
                sitePartInfo.put("syncEth", syncEth2);
                logger.info("Get local site info is: " + sitePartInfo);
                msgs.setData(sitePartInfo.toString());
                rtnFlag = true;
                break;
            case ACTIVE_SET_SITE_INFO:
                String siteVo = jsonParam.getString("siteInfo");
                String siteIplist = JSONObject.parseObject(siteVo).getString(CLUSTER_IPS);
                String masterCluster = jsonParam.getString("masterSite");
                String masterSyncEth = jsonParam.getString("masterSyncEth");
                String masterSiteIps = jsonParam.getString("masterSiteIps");
                String otherToken = jsonParam.getString("token");
                String syncPolicy = jsonParam.getString("syncPolicy");
                String bucketSyncType = jsonParam.getString("bucketSyncType");
                String sslSyncType = jsonParam.getString("sslSyncType");
                String ethTypeKey = jsonParam.getString("ethTypeKey");
                String ethType1 = jsonParam.getString("ethType");
                String syncEth3 = ethTypeToSync(ethTypeKey);

                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd("other_clusters", siteVo);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, SYNC_ETH, syncEth3);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, ETH_TYPE, ethType1);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, CLUSTER_NAME, masterCluster);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, CLUSTER_IPS, masterSiteIps);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, SYNC_POLICY, syncPolicy);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).set(DEFAULT_DATA_SYNC_SWITCH, bucketSyncType);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).set("sync_method", sslSyncType);

                StringBuilder localIps = new StringBuilder();
                for (String node : pool.getCommand(REDIS_NODEINFO_INDEX).keys("*")) {
                    Map<String, String> nodeMap = pool.getCommand(REDIS_NODEINFO_INDEX).hgetall(node);
                    String syncIp = nodeMap.get(ethTypeKey);
                    localIps.append(syncIp).append(",");
                }
                String localSiteIplistpool = localIps.substring(0, localIps.length() - 1);

                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, CLUSTER_IPS, siteIplist + ";" + localSiteIplistpool);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, LINK_STATE, "1");
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, SYNC_STATE, "SYNCED");

                JSONObject masterSiteVo = new JSONObject();
                masterSiteVo.put("siteName", masterCluster);
                masterSiteVo.put("syncPolicy", syncPolicy);
                masterSiteVo.put("vip", jsonParam.getString("vip"));
                masterSiteVo.put("aip", jsonParam.getString("aip"));
                masterSiteVo.put("tokenId", otherToken);
                masterSiteVo.put("syncEth", masterSyncEth);
                //发socket通知cloud，让cloud通知web将站点信息插入数据库
                SocketReqMsg addSqlMsg = new SocketReqMsg(ACTIVE_ADD_MYSQL, 0)
                        .put("masterSiteVo", masterSiteVo.toString())
                        .put("syncEth", syncEth3)
                        .put("ethType", ethType1)
                        .put("bucketSyncType", bucketSyncType)
                        .put("sslSyncType", sslSyncType);
                StringResMsg addSqlRes = sender.sendAndGetResponse(addSqlMsg, StringResMsg.class, true);
                logger.info(" -> -> {}", addSqlMsg);
                if (addSqlRes.getCode() != ErrorNo.SUCCESS_STATUS) {
                    throw new MsException(UNKNOWN_ERROR, "set site info error.");
                }
                msgs.setData("1");
                rtnFlag = true;
                break;
            case ACTIVE_SET_BACK_SITE_INFO: //备份站点
                // 添加other_cluster信息
                String siteVoList = jsonParam.getString("site_info_list");
                String sync3dc = "";
                final JSONArray siteVoArray = JSONArray.parseArray(siteVoList);
                for (Object site : siteVoArray) {
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd("other_clusters", site.toString());
                }
                // 添加主站点信息
                String masterClusterName = jsonParam.getString("masterSite"); // 主站点名称
                String masterSiteIpList = jsonParam.getString("masterSiteIps"); // 主站点后端ip列表
                String masterSyncPolicy = jsonParam.getString("syncPolicy"); // 主站点对站点同步策略
                String masterSyncPolicyAsync = jsonParam.getString("masterSyncPolicyAsync"); //主站点对备站点同步策略
                String clusterSyncWay = jsonParam.getString("clusterSyncWay"); // 多站点同步策略
                String dataSyncType = jsonParam.getString("bucketSyncType");
                String sslType = jsonParam.getString("sslSyncType");
                String ethTypeKey1 = jsonParam.getString("ethTypeKey");
                String ethType2 = jsonParam.getString("ethType");

                String syncEth1 = ethTypeToSync(ethTypeKey1);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, SYNC_ETH, syncEth1);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, ETH_TYPE, ethType2);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, CLUSTER_NAME, masterClusterName);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, CLUSTER_IPS, masterSiteIpList);
                if ("1".equals(clusterSyncWay)) {
                    // 三复制
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, SYNC_POLICY, "1");
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, CLUSTERS_SYNC_WAY, clusterSyncWay);
                } else {
                    // 3dc
                    if ("1".equals(masterSyncPolicyAsync)) {
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, SYNC_POLICY, "0");
                        sync3dc = "2";
                    } else {
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, SYNC_POLICY, "2");
                    }
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, SYNC_POLICY_ASYNC, masterSyncPolicyAsync);
                }

                // 构建local_cluster信息
                // 获取本地的ip列表
                String siteIpList = jsonParam.getString("iplist");
                StringBuilder localIpList = new StringBuilder();
                for (String node : pool.getCommand(REDIS_NODEINFO_INDEX).keys("*")) {
                    Map<String, String> nodeMap = pool.getCommand(REDIS_NODEINFO_INDEX).hgetall(node);
                    String syncIp = nodeMap.get(ethTypeKey1);
                    localIpList.append(syncIp).append(",");
                }
                String localSiteIpListpool = localIpList.substring(0, localIpList.length() - 1);

                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, CLUSTER_IPS, siteIpList + ";" + localSiteIpListpool);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, LINK_STATE, "1");
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, SYNC_STATE, "SYNCED");
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).set(DEFAULT_DATA_SYNC_SWITCH, dataSyncType);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).set("sync_method", sslType);
                // 将部署站点信息写入mysql当中
                final String mysqlSiteInfo = jsonParam.getString("mysqlSiteInfo");
                // 发送socket是ms_cloud
                //发socket通知cloud，让cloud通知web将站点信息插入数据库
                SocketReqMsg sqlMsg = new SocketReqMsg(ACTIVE_ADD_MYSQL, 0)
                        .put("other_clusters", mysqlSiteInfo)
                        .put("bucketSyncType", dataSyncType)
                        .put("sslSyncType", sslType)
                        .put("syncEth", syncEth1)
                        .put("ethType", ethType2)
                        .put(CLUSTERS_SYNC_WAY, sync3dc);
                StringResMsg sqlRes = sender.sendAndGetResponse(sqlMsg, StringResMsg.class, true);
                if (sqlRes.getCode() != ErrorNo.SUCCESS_STATUS) {
                    throw new MsException(UNKNOWN_ERROR, "set site info error.");
                }
                msgs.setData("1");
                rtnFlag = true;
                break;
            case ACTIVE_CHECK_SYNC_BUCKET:
                String checkBucket = jsonParam.getString("checkBucket");
                String newSiteName = jsonParam.getString("newSiteName");
                long time = System.currentTimeMillis() + 12 * 60 * 60 * 1000;
                Set<String> needSyncBuckets = pool.getCommand(REDIS_SYSINFO_INDEX).smembers("need_sync_buckets");
                if ("0".equals(checkBucket) || needSyncBuckets.isEmpty()) {
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del(TEMP_CLUSTERS);
                    JSONObject tempCluster = new JSONObject();
                    tempCluster.put(CLUSTER_NAME, newSiteName);
                    tempCluster.put(CLUSTER_ROLE, "slave");
                    tempCluster.put("tempTime", time);
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd(TEMP_CLUSTERS, tempCluster.toJSONString());
                    // 通知后端包定时扫描临时站点情况
                    SocketReqMsg tempSocket = new SocketReqMsg("startTempSiteScan", 0);
                    sender.sendAndGetResponse(tempSocket, StringResMsg.class, true);
                    logger.info("send msg to ms_cloud scan temp site modify !!!");
                    msgs.setData("1");
                } else {
                    msgs.setData("-1");
                }
                rtnFlag = true;
                break;
            case ACTIVE_REMOTE_TEMP_SITE:
                try {
                    String siteMapStr = jsonParam.getString("siteMap");
                    String ip = jsonParam.getString("ip");
                    Map<String, String> siteMap = JSON.parseObject(siteMapStr, new TypeReference<Map<String, String>>() {
                    });
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).del(TEMP_CLUSTERS);
                    long tempTime = System.currentTimeMillis() + 12 * 60 * 60 * 1000;
                    for (Map.Entry<String, String> entry : siteMap.entrySet()) {
                        JSONObject tempCluster = new JSONObject();
                        tempCluster.put(CLUSTER_NAME, entry.getKey());
                        tempCluster.put(CLUSTER_ROLE, entry.getValue());
                        tempCluster.put("tempTime", tempTime);
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd(TEMP_CLUSTERS, tempCluster.toJSONString());
                    }
                    SocketReqMsg tempSocket = new SocketReqMsg("remoteStartTempSiteScan", 0).put("ip", ip);
                    final StringResMsg resMsg1 = sender.sendAndGetResponse(tempSocket, StringResMsg.class, true);
                    String md5Str = resMsg1.getData();
                    logger.info("getRemoteFile md5: {}", md5Str);
                    md5Str = md5Str.trim();
                    if (md5Str.startsWith("{") && md5Str.endsWith("}")) {
                        md5Str = md5Str.substring(1, md5Str.length() - 1);
                    }
                    Map<String, String> md5Map = new LinkedHashMap<>();
                    // 分割 key=value 对（注意值中可能有空格）
                    String[] entries = md5Str.split(", (?=[^=]+=)");
                    for (String entry : entries) {
                        String[] kv = entry.split("=", 2);
                        if (kv.length == 2) {
                            md5Map.put(kv[0].trim(), kv[1].trim());
                        }
                    }
                    final JSONObject md5Json = new JSONObject();
                    md5Json.put("md5Map", md5Map);
                    msgs.setData(md5Json.toString());
                    logger.info("send msg to ms_cloud scan temp site modify !!!");
                } catch (Exception e) {
                    msgs.setData("-1");
                }
                rtnFlag = true;
                break;
            case ACTIVE_SET_SLAVE_SITE_INFO: //从站点设置第三站点信息
                String siteInfoJson = jsonParam.getString("siteInfo");
                final JSONObject jsonObject = JSONObject.parseObject(siteInfoJson);
                String sync3DC = "";
                if (jsonObject.containsKey(SYNC_POLICY_ASYNC)) {
                    String syncPolicyAsync = jsonObject.getString(SYNC_POLICY_ASYNC);
                    // 设置当前站点syncPolicy
                    if ("1".equals(syncPolicyAsync)) {
                        pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, SYNC_POLICY, "2");
                        sync3DC = "2";
                    }
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, SYNC_POLICY_ASYNC, syncPolicyAsync);
                }
                if (jsonObject.containsKey(CLUSTERS_SYNC_WAY)) {
                    String syncWay = jsonObject.getString(CLUSTERS_SYNC_WAY);
                    // 设置主站点syncPolicy
                    pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(MASTER_CLUSTER, CLUSTERS_SYNC_WAY, syncWay);
                }

                // 设置other_clusters
                String cluster_role = jsonObject.getString("role");
                String cluster_name = jsonObject.getString("site_name");
                String iplist = jsonObject.getString("iplist");
                String syncEth5 = jsonObject.getString("syncEth");
                JSONObject cluster = new JSONObject();
                cluster.put("cluster_role", "0".equals(cluster_role) ? "slave" : "master");
                cluster.put("cluster_name", cluster_name);
                cluster.put("iplist", iplist);
                cluster.put("sync_eth", syncEth5);
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).sadd("other_clusters", JSON.toJSONString(cluster));
                // 设置local_cluster
                final String clusterIps = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_IPS);
                String newClusterIps = clusterIps + ";" + iplist;
                pool.getShortMasterCommand(REDIS_SYSINFO_INDEX).hset(LOCAL_CLUSTER, CLUSTER_IPS, newClusterIps);

                jsonObject.remove(CLUSTER_IPS);
                jsonObject.remove(SYNC_POLICY_ASYNC);
                jsonObject.remove(CLUSTERS_SYNC_WAY);
                jsonObject.remove(CLUSTER_ROLE);
                //发socket通知cloud，让cloud通知web将站点信息插入数据库
                SocketReqMsg backMysqlMsg = new SocketReqMsg(ACTIVE_ADD_MYSQL, 0)
                        .put("backSiteVo", jsonObject.toString())
                        .put(CLUSTERS_SYNC_WAY, sync3DC);
                StringResMsg addBackSqlRes = sender.sendAndGetResponse(backMysqlMsg, StringResMsg.class, true);
                if (addBackSqlRes.getCode() != ErrorNo.SUCCESS_STATUS) {
                    throw new MsException(UNKNOWN_ERROR, "set site info error.");
                }
                msgs.setData("1");
                rtnFlag = true;
                break;
            case ACTIVE_SET_EXTRA_SITE:
                logger.info("getServerLink: " + jsonParam);
                SocketReqMsg extraMsg = new SocketReqMsg(ACTIVE_ADD_EXTRA_MYSQL, 0)
                        .put("extraSiteVo", jsonParam.toString());
                StringResMsg extraRes = sender.sendAndGetResponse(extraMsg, StringResMsg.class, true);
                if (extraRes.getCode() != ErrorNo.SUCCESS_STATUS) {
                    throw new MsException(UNKNOWN_ERROR, "set extra site error.");
                }
                msgs.setData("1");
                rtnFlag = true;
                break;
            case ACTIVE_GET_SITE_STORAGE_STRATEGY:
                // 根据指定策略名称判断该存储策略是否存在
                if (jsonParam.containsKey("strategyName")) {
                    String strategyName = jsonParam.getString("strategyName");
                    Long strategyExist = pool.getCommand(REDIS_POOL_INDEX).exists(strategyName);
                    if (strategyExist != 0) {
                        if (jsonParam.containsKey("strategyDup")) {
                            String strategyDup = jsonParam.getString("strategyDup");
                            String localStrategyDup = Optional.ofNullable(pool.getCommand(REDIS_POOL_INDEX).hget(strategyName, "deduplicate")).orElse("off");
                            if (strategyDup.equals(localStrategyDup)) {
                                msgs.setData("1");
                            } else {
                                msgs.setData("0");
                            }
                        } else {
                            msgs.setData("1");
                        }
                    } else {
                        msgs.setData("0");
                    }
                } else {
                    // 若不指定策略名称则返回默认策略
                    List<String> keys = pool.getCommand(REDIS_POOL_INDEX).keys("strategy_*");
                    String defaultStorageStrategy = null;
                    for (String key : keys) {
                        Map<String, String> strategy = pool.getCommand(REDIS_POOL_INDEX).hgetall(key);
                        String orDefault = strategy.getOrDefault("is_default", "false");
                        if (Boolean.parseBoolean(orDefault)) {
                            defaultStorageStrategy = key;
                            msgs.setData(key);
                            break;
                        }
                    }
                    if (defaultStorageStrategy == null) {
                        msgs.setData("0");
                    }
                }
                rtnFlag = true;
                break;
            case ACTIVE_UPDATE_SITE_DEFAULT_STRATEGY:
                String oldDefaultStrategy = jsonParam.getString("oldDefaultStrategy");
                String newDefaultStrategy = jsonParam.getString("newDefaultStrategy");
                SocketReqMsg updateStrategyMSG = new SocketReqMsg(ACTIVE_UPDATE_SITE_DEFAULT_STRATEGY, 0)
                        .put("oldDefaultStrategy", oldDefaultStrategy)
                        .put("newDefaultStrategy", newDefaultStrategy);

                if (jsonParam.containsKey(SITE_FLAG)) {
                    updateStrategyMSG.put(SITE_FLAG, "1");
                }

                StringResMsg updateStrategyRes = sender.sendAndGetResponse(updateStrategyMSG, StringResMsg.class, true);

                if (updateStrategyRes.getCode() == ErrorNo.INCONSISTENT_STORAGE_STRATEGY) {
                    throw new MsException(ErrorNo.INCONSISTENT_STORAGE_STRATEGY, "The multiple site storage strategy inconsistent.");
                }

                if (updateStrategyRes.getCode() != ErrorNo.SUCCESS_STATUS) {
                    throw new MsException(UNKNOWN_ERROR, "update site default strategy info error.");
                }
                msgs.setData("1");
                rtnFlag = true;
                break;
            case "add_cluster_ip":
                SocketReqMsg addClusterIp = new SocketReqMsg("add_cluster_ip", 0)
                        .put("param", jsonParam.toString());
                StringResMsg addClusterIpRes = sender.sendAndGetResponse(addClusterIp, StringResMsg.class, true);
                msgs.setData(addClusterIpRes.getData());
                rtnFlag = true;
                break;
            case "delete_cluster_ip":
                SocketReqMsg deleteClusterIp = new SocketReqMsg("delete_cluster_ip", 0)
                        .put("param", jsonParam.toString());
                StringResMsg delClusterIpRes = sender.sendAndGetResponse(deleteClusterIp, StringResMsg.class, true);
                msgs.setData(delClusterIpRes.getData());
                rtnFlag = true;
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
        if (rtnFlag) {
            return msgs;
        } else {
            return new ResponseMsg(ErrorNo.SUCCESS_STATUS);
        }
    }

    private static void setBucketStatus(String bucket, String ak, String sk, Map<String, Set<Integer>> syncMap, Map<String, Set<Integer>> archiveMap) {
        if (syncMap != null) {
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket, SYNC_INDEX, JSON.toJSONString(syncMap));
        }
        if (archiveMap != null) {
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket, ARCHIVE_INDEX, JSON.toJSONString(archiveMap));
        }
        if (StringUtils.isNotBlank(ak) && StringUtils.isNotBlank(sk)) {
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket, "server_ak", ak);
            pool.getShortMasterCommand(REDIS_BUCKETINFO_INDEX).hset(bucket, "server_sk", sk);
        }
    }

    public String ethTypeToSync(String ethTypeKey) {
        String syncEth;
        if (BUSINESS_ETH1.equals(ethTypeKey)) {
            syncEth = "eth2";
        } else if (BUSINESS_ETH2.equals(ethTypeKey)) {
            syncEth = "eth3";
        } else if (BUSINESS_ETH3.equals(ethTypeKey)) {
            syncEth = "eth18";
        } else if (BUSINESS_ETH1_IPV6.equals(ethTypeKey)) {
            syncEth = "eth2";
        } else if (BUSINESS_ETH2_IPV6.equals(ethTypeKey)) {
            syncEth = "eth3";
        } else if (BUSINESS_ETH3_IPV6.equals(ethTypeKey)) {
            syncEth = "eth18";
        } else if (HEART_ETH1.equals(ethTypeKey)) {
            syncEth = "eth4";
        } else {
            syncEth = "eth12";
        }
        return syncEth;
    }

    /**
     * 通知IAM更新角色
     *
     * @return
     */
    private static int infoIam() {
        SocketReqMsg msg = new SocketReqMsg("deployRegionFinish", 0);
        MapResMsg resMsg = sender.sendAndGetResponse(getWebAddr(), msg, MapResMsg.class, true);
        logger.info("INFORM IAM'S RESULT IS " + resMsg.getCode());
        return resMsg.getCode();
    }

    public static void checkEtcdCompress(long currentTime) {
        long cacheCount = EtcdClient.getCountByPrefix("/compress/");
        if (cacheCount == 0) {
            return;
        }
        List<KeyValue> keyValues = EtcdClient.selectByPrefixWithAscend("/compress/");
        long time = 5 * 60 * 1000;
        if (cacheCount >= 10000) {
            time = 10 * 1000;
        } else if (cacheCount >= 100) {
            time = 60 * 1000;
        }
        for (KeyValue kv : keyValues) {
            String key = new String(kv.getKey().getBytes());
            String value = new String(kv.getValue().getBytes());
            String bucket = key.split("/")[2];
            String syncType = pool.getCommand(REDIS_BUCKETINFO_INDEX).hget(bucket, "sync_compress");
            com.alibaba.fastjson.JSONObject cacheJson = com.alibaba.fastjson.JSONObject.parseObject(value);
            String newSyncType = cacheJson.getString("syncType");
            long cacheTime = Long.parseLong(cacheJson.getString("syncTime"));
            boolean equals = newSyncType.equals(syncType);
            if (equals && currentTime - cacheTime > time) {
                EtcdClient.delete(key);
            }
        }
    }

    /**
     * ping ip
     *
     * @param ipList ip
     * @return 通：true，不通：false
     */
    private static boolean isReachable(String ipList) {
        boolean status = false;
        for (String ip : ipList.split(",")) {
            for (int tryTime = 2; ; tryTime -= 1) {
                try {
                    status = InetAddress.getByName(ip).isReachable(5000);
                    if (status || tryTime == 0) {
                        break;
                    }
                } catch (Exception e) {
                    logger.error("ping ip error.ip: " + ip, e);
                    status = false;
                    if (tryTime == 0) {
                        break;
                    }
                }
            }
            if (!status) {
                break;
            }
        }

        return status;
    }
}
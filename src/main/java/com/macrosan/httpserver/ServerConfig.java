package com.macrosan.httpserver;

import com.macrosan.constants.SysConstants;
import com.macrosan.database.etcd.EtcdClient;
import com.macrosan.database.redis.ReadWriteLock;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.utils.property.PropertyReader;
import io.vertx.reactivex.core.Vertx;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;

/**
 * ServerConfig
 * 配置http服务器,非线程安全的单例模式
 *
 * @author liyixin
 * @date 2018/10/29
 */
@Log4j2
public class ServerConfig {

    private static final Logger logger = LogManager.getLogger(ServerConfig.class);

    @Getter
    private String dns = null;

    @Getter
    private long maxVnodeNum = 512000;

    @Getter
    private final String hostUuid;

    @Getter
    private final String dateCheck;

    @Getter
    private final String masterVip1;

    @Getter
    private final String masterVip2;

    @Getter
    private final String heartIp1;

    @Getter
    private final String heartIp2;

    @Getter
    private final String bindIp1;

    @Getter
    private final String bindIp2;

    @Getter
    private final String bindIpV61;

    @Getter
    private final String bindIpV62;

    @Getter
    private final Vertx vertx;

    @Getter
    private final String sp;

    @Getter
    private String region;

    @Getter
    private String site;

    @Getter
    private String dnsSuffix = null;

    @Getter
    private String dnsPrefix = null;

    @Getter
    private String accessIp = null;

    @Getter
    private String accessIpV6 = null;

    @Getter
    private String mapState;

    @Getter
    private String state;

    @Getter
    private String tempState;

    @Getter
    private String httpsPort;

    @Getter
    private String httpPort;

    @Getter
    private static boolean isBucketUpper;

    private final boolean isUnify;

    private final boolean isVm;

    @Getter
    private String eth4;

    @Getter
    // 0 不开启 1 开启 2 部分桶开启
    private int quickReturn;

    public static boolean isUnify() {
        return getInstance().isUnify;
    }

    public static boolean isVm() {
        return getInstance().isVm;
    }

    /**
     * 用来探测查询参数的哈希冲突
     */
    private final List<String> guardList = new ArrayList<>();

    private final IntArrayList manageRouteList = new IntArrayList();

    private final IntArrayList dataRouteList = new IntArrayList();

    private static ServerConfig instance = null;

    private ServerConfig(String[] args, Vertx vertx) {

        this.vertx = vertx;

        bindIp1 = args[0];
        bindIp2 = args[1];

        bindIpV61 = args.length > 2 ? args[2] : "";
        bindIpV62 = args.length > 2 ? args[3] : "";
        ReadWriteLock.readLock(false);
        PropertyReader reader = new PropertyReader(PUBLIC_CONF_FILE);
        ReadWriteLock.unLock(false);
        ReadWriteLock.release();
        isUnify = "unify".equalsIgnoreCase(reader.getPropertyAsString("server"));
        isVm = "mossvm".equalsIgnoreCase(reader.getPropertyAsString("server"));
        if (isUnify) {
            SysConstants.HEART_ETH1 = "heartbeat_MS_Cloud";
            SysConstants.HEART_ETH2 = "heartbeat_MS_Cloud";
            SysConstants.NODE_SERVER_STATE = "server_state_MS_C";
            SysConstants.HEART_IP = "heartbeat_MS_Cloud";
        }

        masterVip1 = reader.getPropertyAsString("master_vip1");
        masterVip2 = reader.getPropertyAsString("master_vip2");
        heartIp1 = isUnify ? reader.getPropertyAsString("vm_ip_ms_cloud")
                : reader.getPropertyAsString("vm_ip_eth4");
        heartIp2 = isUnify ? reader.getPropertyAsString("vm_ip_ms_cloud")
                : reader.getPropertyAsString("vm_ip_eth5");

        hostUuid = reader.getPropertyAsString("vm_uuid");
        dateCheck = reader.getPropertyAsString("date_check");
        sp = reader.getPropertyAsString("sp");
        mapState = reader.getPropertyAsString("map_state");
        state = reader.getPropertyAsString("state");
        tempState = reader.getPropertyAsString("temp_state");
        accessIp = reader.getPropertyAsString("access_ip");
        accessIpV6 = reader.getPropertyAsString("access_ipv6");

        if (System.getProperty("com.macrosan.takeOver") == null) {
            eth4 = reader.getPropertyAsString("vm_ip_eth4");
        } else {
            PropertyReader peer = new PropertyReader(PUBLIC_PEER_CONF_FILE);
            eth4 = peer.getPropertyAsString("vm_ip_eth4");
        }
    }

    public static ServerConfig getInstance() {
        return instance;
    }

    public static void init(String[] args, Vertx vertx) {
        if (instance == null) {
            instance = new ServerConfig(args, vertx);
        }
    }

    public void initValueFromRedis() {
        this.dns = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get("dns_name");
//        this.maxVnodeNum = Long.parseLong(RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get(V_NUM));
//        this.dns = "test.com";
        ReadWriteLock.readLock(false);
        PropertyReader reader = new PropertyReader(PUBLIC_CONF_FILE);
        ReadWriteLock.unLock(false);
        ReadWriteLock.release();
        this.maxVnodeNum = Long.parseLong(reader.getPropertyAsString("v_num"));
        this.region = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hget(MULTI_REGION_LOCAL_REGION, REGION_NAME);
        this.site = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        this.dnsSuffix = EtcdClient.getDnsSuffix();
        this.dnsPrefix = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get("dns_name_prefix");
        this.httpPort = Optional.ofNullable(RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get("http_port")).orElse("80");
        this.httpsPort = Optional.ofNullable(RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get("https_port")).orElse("443");
        String quickReturn = Optional.ofNullable(RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get("quickReturn")).orElse("0");
        this.quickReturn = Integer.parseInt(quickReturn);
        initDnsList(RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get("dns_name"));
        initDnsList(RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get("dns_name1"));
        initDnsList(RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get("dns_name2"));
        if (RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).exists("eth2" + BUSINESS_VLAN_DNS_SUFFIX) == 1) {
            Map<String, String> vlanDnsName = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hgetall("eth2" + BUSINESS_VLAN_DNS_SUFFIX);
            for (Map.Entry<String, String> entry : vlanDnsName.entrySet()) {
                initDnsList(entry.getValue());
            }
        }
        if (RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).exists("eth3" + BUSINESS_VLAN_DNS_SUFFIX) == 1) {
            Map<String, String> vlanDnsName = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hgetall("eth3" + BUSINESS_VLAN_DNS_SUFFIX);
            for (Map.Entry<String, String> entry : vlanDnsName.entrySet()) {
                initDnsList(entry.getValue());
            }
        }
        if (RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).exists("eth18" + BUSINESS_VLAN_DNS_SUFFIX) == 1) {
            Map<String, String> vlanDnsName = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hgetall("eth18" + BUSINESS_VLAN_DNS_SUFFIX);
            for (Map.Entry<String, String> entry : vlanDnsName.entrySet()) {
                initDnsList(entry.getValue());
            }
        }
        this.isBucketUpper = "on".equals(RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get("bucketUpper"));
        globalVariablesReload();
    }

    private void initDnsList(String dnsNames) {
        if (StringUtils.isNotEmpty(dnsNames)) {
            String[] dnsSplit = dnsNames.split(",");
            for (String host : dnsSplit) {
                String[] split = host.split("\\.", 2);
                String regionHost = split[0] + "." + region + "." + split[1];
                String clusterHost = split[0] + "." + site + "." + region + "." + split[1];
                HOST_LIST.add(host);
                ONLY_HOST_LIST.add(host);
                HOST_LIST.add(regionHost);
                HOST_LIST.add(clusterHost);
            }
        }
    }

    /**
     * 填充{@code INCLUDE_PARAM_LIST}
     *
     * @param values 查询参数的值
     */
    private void fillParamList(String... values) {
        for (int i = 1; i < values.length; i++) {
            String value = values[i];
            int hashCode = value.hashCode();
            //hash值都不相同，代表该参数目前不存在
            if (!INCLUDE_PARAM_LIST.contains(hashCode)) {
                INCLUDE_PARAM_LIST.add(hashCode);
                guardList.add(value);
            } else if (!guardList.contains(value)) {
                //有hash值但是字符串不存在，说明产生了hash冲突
                logger.error("hash conflict detected, process will exit.");
                System.exit(-1);
            }
        }
    }

    /**
     * 根据属性文件计算路由map并且填充{@code INCLUDE_PARAM_LIST}
     *
     * @param property 对应属性文件的map
     * @return 计算之后的路由map
     */
    private IntIntHashMap initMapByProperty(UnifiedMap<String, String> property) {
        IntIntHashMap res = new IntIntHashMap((int) (property.size() / 0.75) + 1);
        property.forEachKeyValue((key, value) -> {
                    fillParamList(value.split("[?&]"));
                    String value0 =
                            value.contains("?")
                                    ? value.split("\\?")[0].replaceAll("bucket|object", "") + "?" + value.split("\\?")[1]
                                    : value.replaceAll("bucket|object", "");
                    res.put(value0.hashCode(), key.hashCode());
                }
        );
        return res;
    }

    public IntIntHashMap getManageRoute() {
        PropertyReader manageReader = new PropertyReader(MANAGE_ROUTE);
        IntIntHashMap intIntHashMap = initMapByProperty(manageReader.getPropertyAsMap());
        intIntHashMap.forEachKey(manageRouteList::add);
        return intIntHashMap;
    }

    public IntIntHashMap getDataRoute() {
        PropertyReader dataReader = new PropertyReader(DATA_ROUTE);
        IntIntHashMap intIntHashMap = initMapByProperty(dataReader.getPropertyAsMap());
        intIntHashMap.forEachKey(dataRouteList::add);
        return intIntHashMap;
    }

    public UnifiedMap<String, String> getAction() {
        PropertyReader dataReader = new PropertyReader(ACTION_MAP);
        UnifiedMap<String, String> map = dataReader.getPropertyAsMap();
        return map;
    }

    public UnifiedMap<String, String> getSourceAction() {
        PropertyReader dataReader = new PropertyReader(ACTION_MAP);
        UnifiedMap<String, String> property = dataReader.getPropertyAsMap();
        UnifiedMap<String, String> res = new UnifiedMap<>((int) (property.size() / 0.75) + 1);
        property.forEachKeyValue((key, value) -> {
                    res.put(value, key);
                }
        );
        return res;
    }

    Boolean existManageRoute(String sign) {
        return manageRouteList.contains(sign.hashCode());
    }

    Boolean existDataRoute(String sign) {
        return dataRouteList.contains(sign.hashCode());
    }

    private static void globalVariablesReload() {
        try {
            ServerConfig.getInstance().isBucketUpper = "on".equals(RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).get("bucketUpper"));
        } catch (Exception e) {
            log.error("update global variables error.", e);
        } finally {
            ErasureServer.DISK_SCHEDULER.schedule(ServerConfig::globalVariablesReload, 30, TimeUnit.SECONDS);
        }
    }
}

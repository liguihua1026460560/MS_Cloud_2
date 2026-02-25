package com.macrosan.doubleActive;

import com.alibaba.fastjson.JSON;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.Utils;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DataSynChecker.SCAN_TIMER;
import static com.macrosan.doubleActive.HeartBeatChecker.AVAIL_UUID_MAP;
import static com.macrosan.httpserver.MossHttpClient.*;

@Log4j2
public class MainNodeSelector {
    private final RedisConnPool pool = RedisConnPool.getInstance();

    public static MainNodeSelector instance;

    private static Function<Void, Boolean> checkIfSyncNodeFunc;

    public static MainNodeSelector getInstance() {
        if (instance == null) {
            instance = new MainNodeSelector();
        }
        return instance;
    }

    private boolean isSyncNode = false;

    void init() {
        if (USE_ETH4) {
            // 异步复制使用eth4
            log.info("use eth4 as sync net port.");
            syncNodeSelfCheckEth4();
            checkIfSyncNodeFunc = v -> isSyncNode;
        } else {
            // 异步复制使用eth12。
            // 主要考虑主节点eth4一定正常但eth12不一定。此时扫描流程需要切换到其他非主节点。
            log.info("use eth12 as sync net port.");
            syncNodeSelfCheckEth12();
            checkIfSyncNodeFunc = v -> isSyncNode;
        }
    }

    private void syncNodeSelfCheckEth4() {
        SCAN_TIMER.scheduleAtFixedRate(() -> {
            isSyncNode = "master".equals(Utils.getRoleState()) && pool.getCommand(REDIS_SYSINFO_INDEX).exists(MASTER_CLUSTER) != 0;
        }, 0, 5, TimeUnit.SECONDS);
    }

    /**
     * 定期执行，判断自己是否为扫描节点。
     */
    private void syncNodeSelfCheckEth12() {
        SCAN_TIMER.scheduleAtFixedRate(() -> {
            try {
                String availIp = "";
                String nodeListStr = pool.getCommand(REDIS_SYSINFO_INDEX).get("node_list");
                for (String nodeId : JSON.parseArray(nodeListStr, String.class)) {
                    if (!AVAIL_UUID_MAP.contains(nodeId)) {
                        continue;
                    }
                    Map<String, String> nodeMap = pool.getCommand(REDIS_NODEINFO_INDEX).hgetall(nodeId);
                    if (!"eth12".equals(SYNC_ETH_NAME)) {
                        String syncIpState = pool.getCommand(REDIS_SYSINFO_INDEX).hget(SYNC_ETH_NAME + "_status", nodeId);
                        if ("1".equals(nodeMap.get(NODE_SERVER_STATE)) && syncIpState.startsWith("1")) {
                            String syncEthToType = syncEthToType(SYNC_ETH_NAME, SYNC_ETH_TYPE);
                            availIp = nodeMap.get(syncEthToType);
                            break;
                        }
                    } else {
                        String syncIpState = nodeMap.get(ACTIVE_SYNC_IP_STATE);
                        if ("1".equals(nodeMap.get(NODE_SERVER_STATE)) && "up".equals(syncIpState)) {
                            availIp = nodeMap.get(ACTIVE_SYNC_IP);
                            break;
                        }
                    }
                }
                if (StringUtils.isEmpty(availIp)) {
                    isSyncNode = "master".equals(Utils.getRoleState());
                } else {
                    isSyncNode = availIp.equals(LOCAL_NODE_IP);
                }
            } catch (Exception e) {
                log.error("check if sync node error.", e);
            }
        }, 0, 5, TimeUnit.SECONDS);

    }

    public static boolean checkIfSyncNode() {
        return checkIfSyncNodeFunc.apply(null);
    }

}

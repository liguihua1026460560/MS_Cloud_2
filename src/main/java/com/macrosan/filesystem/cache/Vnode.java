package com.macrosan.filesystem.cache;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.ec.VersionUtil;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.cache.Vnode.VnodeState.State;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.socketmsg.SocketReqMsg;
import com.macrosan.rabbitmq.RabbitMqUtils;
import com.macrosan.storage.NodeCache;
import com.macrosan.storage.client.ClientTemplate;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsExecutor;
import io.vertx.core.json.Json;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.stream.Collectors;

import static com.macrosan.ec.server.ErasureServer.PayloadMetaType.*;
import static com.macrosan.message.jsonmsg.Inode.RETRY_INODE;

@Log4j2
public class Vnode {
    public static boolean FS_VNODE_STATE_DEBUG = false;

    public static final int HEART_MIL = 1000;
    public static final long HEART_TIMEOUT_NAO = 5000_000_000L;
    public static final int UP_WAIT_NUM = 7;
    public static final String INODE_HEART_DOWN_ERROR = "closed connection";

    static class VnodeState {
        ReadLock readLock;
        WriteLock writeLock;
        long heartTime = -1;
        State state = State.down;
        String masterNode = null;
        int stateVersion = 0;
        int upNum = 0;

        VnodeState() {
            ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
            readLock = lock.readLock();
            writeLock = lock.writeLock();
        }

        enum State {
            master,
            up,
            slave,
            down
        }
    }

    int num;
    String curNode;

    //当前Vnode对应的索引池的vnode号
    int storageVnode;
    final VnodeState state = new VnodeState();
    Map<String, Long> lastHeartMap = new ConcurrentHashMap<>();
    Set<String> runningNodeSet = ConcurrentHashMap.newKeySet();
    Set<Long> runningInodeSet = new HashSet<>();
    InodeCache inodeCache;
    MsExecutor executor;
    List<Tuple3<String, String, String>> cachedNodeList;
    AtomicBoolean enable = new AtomicBoolean(false);

    public Vnode(Node node, int num) {
        executor = node.executor;
        this.num = num;
        this.curNode = node.node;
        inodeCache = new InodeCache(ErasureServer.DISK_SCHEDULER);

        double multi = Node.STORAGE_POOL.getVnodeNum() * 1.0 / Node.TOTAL_V_NUM;
        storageVnode = (int) (num * multi);

        if (storageVnode >= Node.STORAGE_POOL.getVnodeNum()) {
            storageVnode = Node.STORAGE_POOL.getVnodeNum() - 1;
        }

        cachedNodeList = Node.STORAGE_POOL.mapToNodeInfo(String.valueOf(storageVnode)).block();
        boolean needStart = false;
        for (Tuple3<String, String, String> tuple3 : cachedNodeList) {
            if (tuple3.var1.equals(RabbitMqUtils.CURRENT_IP)) {
                needStart = true;
                break;
            }
        }

        if (needStart) {
            if (enable.compareAndSet(false, true)) {
                executor.schedule(() -> heart(cachedNodeList), 1, TimeUnit.SECONDS);
            }
        } else {
            enable.set(false);
            executor.schedule(() -> slave(cachedNodeList), 1, TimeUnit.SECONDS);
        }
    }

    public int getPriority(List<Tuple3<String, String, String>> nodeList, String ip) {
        for (int i = 0; i < nodeList.size(); i++) {
            Tuple3<String, String, String> tuple3 = nodeList.get(i);
            if (tuple3.var1.equals(ip)) {
                return i;
            }
        }

        return -1;
    }

    public List<Tuple3<String, String, String>> getCachedNodeList() {
        return cachedNodeList;
    }

    public void tryVote() {
        try {
            List<Tuple3<String, String, String>> nodeList = Node.STORAGE_POOL.mapToNodeInfo(String.valueOf(storageVnode)).block();
            if (isNotMigrate(cachedNodeList, nodeList)) {
                vote(cachedNodeList);
            } else {
                String masterNode = state.masterNode;
                if (null == masterNode) {
                    // 旧主迁移后，旧关联组范围内记录的masterNode为空，此时确认其它节点的状态是否全部为down，是则更新nodeList，不是继续等待
                    // 确认时若对端节点的list已经更新，则直接将之视为down状态(即跳过对该节点的判断)
                    AtomicInteger downNum = new AtomicInteger(0);
                    AtomicBoolean masterExist = new AtomicBoolean(false);
                    List<SocketReqMsg> msgList = nodeList.stream().map(tuple3 -> {
                        return new SocketReqMsg("", 0)
                                .put("vNum", String.valueOf(num))
                                .put("priority", String.valueOf(-1))
                                .put("node", curNode)
                                .put("getState", "1");
                    }).collect(Collectors.toList());
                    ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgList, INODE_CACHE_HEART, String.class, cachedNodeList);
                    responseInfo.responses.subscribe(t -> {
                        if (t.var2 == SUCCESS) {
                            String[] array = t.var3.split("_");
                            String state = array[0];
                            String pickedCachedList = array[1];
                            String pickedCurList = array[2];

                            if (pickIpAndVnode(nodeList).equals(pickedCurList) && pickedCachedList.equals(pickedCurList)) {
                                // 如果查询节点的list已经更新，则无论state是否为down，均视为down，以使当前节点及时更新list
                                downNum.incrementAndGet();
                            } else {
                                // 如果查询节点的list还没有更新，则根据state进行判断
                                if (State.down.name().equals(state) || State.slave.name().equals(state)) {
                                    downNum.incrementAndGet();
                                } else if (State.master.name().equals(state)) {
                                    masterExist.set(true);
                                }
                            }
                        } else {
                            downNum.incrementAndGet();
                        }
                    }, e -> {
                        log.error("", e);
                    }, () -> {
                        if (downNum.get() == cachedNodeList.size() || !masterExist.get()) {
                            cachedNodeList = nodeList;
                            vote(cachedNodeList);
                        } else {
                            vote(cachedNodeList);
                        }
                    });
                } else {
                    // 发生节点间的迁移，主迁移则需先在旧关联组范围降为备，再在新关联组范围内选主
                    // 备迁移则直接在新关联组范围选主
                    String masterIP = NodeCache.getIP(masterNode);
                    int migrateCase = getMigrateCase(masterIP);
                    if (migrateCase >= 0) {
                        if (migrateCase == 1) {
                            // 主节点vnode发生迁移，迁移时旧主仍然是主，旧备一直是备，备状态不会发heart，只有主会发heart，此时旧备检测到旧主发生
                            // 迁移，返"0"给旧主，从而使旧主和旧备先后降为down，全部降为down后更新nodeList选举新主
                            AtomicReference<String> newMasterState = new AtomicReference<>("");
                            List<Tuple3<String, String, String>> singleList = Collections.singletonList(new Tuple3<>(masterIP, "fs-SP0-0", "0"));
                            List<SocketReqMsg> msgList = Collections.singletonList(new SocketReqMsg("", 0)
                                    .put("vNum", String.valueOf(num))
                                    .put("priority", String.valueOf(-1))
                                    .put("node", curNode)
                                    .put("getState", "1"));

                            ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgList, INODE_CACHE_HEART, String.class, singleList);
                            responseInfo.responses.subscribe(t -> {
                                if (t.var2 == SUCCESS) {
                                    String[] array = t.var3.split("_");
                                    String state = array[0];
                                    newMasterState.set(state);
                                }
                            }, e -> {
                                log.error("", e);
                            }, () -> {
                                if (responseInfo.successNum == 1) {
                                    // 如果此时的主已经是down，说明主已经降为了备，此时更新nodeList
                                    if (State.down.name().equals(newMasterState.get())) {
                                        cachedNodeList = nodeList;
                                        vote(cachedNodeList);
                                    } else {
                                        vote(cachedNodeList);
                                    }
                                } else {
                                    vote(cachedNodeList);
                                }
                            });
                        } else {
                            //备节点vnode发生迁移，此时旧主仍然主，可以直接切换
                            cachedNodeList = nodeList;
                            vote(cachedNodeList);
                        }
                    } else {
                        //没有节点发生迁移
                        cachedNodeList = nodeList;
                        vote(cachedNodeList);
                    }
                }
            }
        } catch (Exception e) {
            log.error("try vote error", e);
            executor.schedule(this::tryVote, HEART_MIL, TimeUnit.MILLISECONDS);
        }
    }

    public void vote(List<Tuple3<String, String, String>> list) {
        boolean needStart = false;
        for (Tuple3<String, String, String> tuple3 : list) {
            if (tuple3.var1.equals(RabbitMqUtils.CURRENT_IP)) {
                needStart = true;
                break;
            }
        }

        if (needStart) {
            enable.compareAndSet(false, true);
            heart(list);
        } else {
            enable.compareAndSet(true, false);
            slave(list);
        }
    }

    /**
     * 比较 oldList 和 newList的var1是否全部一致，不一致说明存在节点间的vnode迁移
     **/
    public static boolean isNotMigrate(List<Tuple3<String, String, String>> oldList, List<Tuple3<String, String, String>> newList) {
        // 默认是旧ip在新ip中存在
        boolean isOldExist = true;
        for (Tuple3<String, String, String> oldTuple : oldList) {
            String oldIp = oldTuple.var1;
            boolean isExist = false;
            for (Tuple3<String, String, String> newTuple : newList) {
                String newIp = newTuple.var1;
                // 当前旧ip在新ip中存在，直接检查下一个旧ip
                if (oldIp.equals(newIp)) {
                    isExist = true;
                    break;
                }
            }

            // 新ip中不存在旧ip，置为false
            if (!isExist) {
                isOldExist = false;
                break;
            }
        }

        boolean isNewExist = true;
        for (Tuple3<String, String, String> newTuple : newList) {
            String newIp = newTuple.var1;
            boolean isExist = false;
            for (Tuple3<String, String, String> oldTuple : oldList) {
                String oldIp = oldTuple.var1;
                if (newIp.equals(oldIp)) {
                    isExist = true;
                    break;
                }
            }

            if (!isExist) {
                isNewExist = false;
                break;
            }
        }

        return isOldExist && isNewExist;
    }

    /**
     * 根据新旧nodeList判断旧节点中的哪个节点发生了节点间迁移
     *
     * @return 返回 0 表示没有节点发生迁移
     * 返回 1 表示旧主发生了迁移
     * 返回 2 表示旧备发生了迁移
     **/
    public int getMigrateCase(String masterIP) {
        // 正常情况下主发送给备，备返回消息
        List<Tuple3<String, String, String>> curList = Node.STORAGE_POOL.mapToNodeInfo(String.valueOf(storageVnode)).block();

        // 找到旧主的位置
        int masterIndex = -1;
        for (int i = 0; i < cachedNodeList.size(); i++) {
            if (cachedNodeList.get(i).var1.equals(masterIP)) {
                masterIndex = i;
                break;
            }
        }

        // 找到发生迁移的位置，迁移vnode后，vnode在nodeList的位置是不变的，但是ip会改变，查找ip不一样的位置就可以找到发生迁移的位置
        int moveIndex = -1;
        for (int i = 0; i < curList.size(); i++) {
            if (!curList.get(i).var1.equals(cachedNodeList.get(i).var1)) {
                moveIndex = i;
                break;
            }
        }

        if (moveIndex >= 0) {
            // 发生了迁移
            if (moveIndex == masterIndex) {
                // 旧主发生了迁移
                return 1;
            } else {
                // 旧备发生了迁移
                return 2;
            }
        } else {
            // 没有发生迁移
            return 0;
        }
    }

    /**
     * 判断发来heart请求的节点，是否还存在于当前 vnode 对应 storageVnode 的关联组中
     **/
    public boolean isVnodeExist(List<Tuple3<String, String, String>> curList, String node) {
        try {
            boolean res = false;
            String queryIp = NodeCache.getIP(node);
            for (Tuple3<String, String, String> t : curList) {
                if (queryIp.equals(t.var1)) {
                    res = true;
                    break;
                }
            }

            return res;
        } catch (Exception e) {
            log.error("", e);
            return true;
        }
    }

    public String getVnodeStateAndList() {
        List<Tuple3<String, String, String>> curList = Node.STORAGE_POOL.mapToNodeInfo(String.valueOf(storageVnode)).block();
        return getVnodeState() + "_" + pickIpAndVnode(cachedNodeList) + "_" + pickIpAndVnode(curList);
    }

    public static String pickIpAndVnode(List<Tuple3<String, String, String>> list) {
        List<Tuple2<String, String>> res = new LinkedList<>();
        for (int i = 0; i < list.size(); i++) {
            Tuple3<String, String, String> tuple3 = list.get(i);
            res.add(i, new Tuple2(tuple3.var1, tuple3.var3));
        }

        return res.toString();
    }

    public static String getVoteMaster(Map<String, Integer> remoteMaster, List<Tuple3<String, String, String>> nodeList) {
        String res = null;
        int max = 0;
        for (String master : remoteMaster.keySet()) {
            int count = remoteMaster.get(master);
            if (count > max) {
                max = count;
                res = master;
            }
        }

        if (StringUtils.isNotBlank(res) && max > nodeList.size() / 2) {
            return res;
        }

        return null;
    }

    public void tryEnd() {
        enable.compareAndSet(true, false);
    }

    public InodeCache getInodeCache() {
        return this.inodeCache;
    }

    public String getVnodeState() {
        return state.state.name();
    }

    public boolean isMaster() {
        return state.state == State.master;
    }

    public void slave(List<Tuple3<String, String, String>> list) {
        if (!enable.get()) {
            try {
                List<Tuple3<String, String, String>> nodeList;
                List<SocketReqMsg> msgList;

                if (StringUtils.isNotBlank(state.masterNode)) {
                    String ip = NodeCache.getIP(state.masterNode);
                    nodeList = Collections.singletonList(new Tuple3<>(ip, "fs-SP0-0", "0"));
                    msgList = Collections.singletonList(new SocketReqMsg("", 0)
                            .put("vNum", String.valueOf(num))
                            .put("priority", String.valueOf(-1)));

                } else {
                    if (list == null) {
                        list = Node.STORAGE_POOL.mapToNodeInfo(String.valueOf(storageVnode)).block();
                    }

                    nodeList = list;
                    msgList = nodeList.stream().map(tuple3 -> new SocketReqMsg("", 0)
                            .put("vNum", String.valueOf(num))
                            .put("priority", String.valueOf(-1)))
                            .collect(Collectors.toList());
                }

                ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgList, INODE_CACHE_HEART, String.class, nodeList);
                responseInfo.responses.subscribe(t -> {
                    if (t.var2 == SUCCESS) {
                        state.masterNode = t.var3;
                    }
                }, e -> {
                    log.error("", e);
                }, () -> {
                    if (responseInfo.successNum == 0) {
                        state.masterNode = null;
                    }
                });
            } finally {
                executor.schedule(this::tryVote, HEART_MIL, TimeUnit.MILLISECONDS);
            }
        }
    }

    public void heart(List<Tuple3<String, String, String>> list) {
        long startVersion = state.stateVersion;
        if (enable.get()) {
            try {
                if (list == null) {
                    list = Node.STORAGE_POOL.mapToNodeInfo(String.valueOf(storageVnode)).block();
                }

                List<Tuple3<String, String, String>> nodeList = list;

                if (state.state == State.slave) {
                    long curTime = System.nanoTime();
                    if (curTime - state.heartTime >= HEART_TIMEOUT_NAO) {
                        heartFail(startVersion, null);
                    } else {
                        heartSuccess(startVersion, 0, nodeList);
                    }
                } else {
                    runningNodeSet.clear();

                    List<SocketReqMsg> msgList = nodeList.stream().map(tuple3 -> {
                        long lastHeart = lastHeartMap.getOrDefault(tuple3.var1, -1L);
                        return new SocketReqMsg("", 0)
                                .put("lastHeart", String.valueOf(lastHeart))
                                .put("priority", String.valueOf(getPriority(nodeList, RabbitMqUtils.CURRENT_IP)))
                                .put("vNum", String.valueOf(num))
                                .put("node", curNode);
                    })
                            .collect(Collectors.toList());

                    AtomicInteger heartSuccessNum = new AtomicInteger();
                    AtomicInteger closeNum = new AtomicInteger();
                    AtomicInteger voteNum = new AtomicInteger();
                    Map<String, Integer> remoteMaster = new HashMap<>(1);
                    ClientTemplate.ResponseInfo<String> responseInfo = ClientTemplate.oneResponse(msgList, INODE_CACHE_HEART, String.class, nodeList);
                    responseInfo.responses.subscribe(t -> {
                        if (t.var2 == SUCCESS) {
                            String ip = nodeList.get(t.var1).var1;
                            long heart = 0L;
                            if (t.var3.indexOf("#") > 0) {
                                String[] split = t.var3.split("#");
                                heart = Long.parseLong(split[0]);
                                String inodeStr = split[1];
                                if (StringUtils.isNotBlank(inodeStr)) {
                                    try {
                                        runningInodeSet = Json.decodeValue(inodeStr, new TypeReference<Set<Long>>() {
                                        });
                                    } catch (Exception e) {

                                    }
                                }
                            } else {
                                //特定情况：加节点重启节点，redis映射更新但内存未更新，新范围内主为down，且masterNode为null
                                if (StringUtils.isNotBlank(t.var3) && t.var3.contains("@")) {
                                    String[] arr = t.var3.split("@");
                                    heart = Long.parseLong(arr[0]);
                                    String master = arr[1];
                                    int c = remoteMaster.getOrDefault(master, 0);
                                    remoteMaster.put(master, c + 1);
                                    voteNum.incrementAndGet();
                                } else {
                                    heart = Long.parseLong(t.var3);
                                }
                            }
                            if (heart < 0) {
                                runningNodeSet.add(ip);
                                heart = -heart;
                            }

                            if (heart != 0) {
                                heartSuccessNum.incrementAndGet();
                                lastHeartMap.put(ip, heart);
                            }
                        }
                        if (!state.state.equals(State.master)
                                && t.var2 == ERROR &&
                                INODE_HEART_DOWN_ERROR.equals(t.var3)) {
                            closeNum.incrementAndGet();
                        }
                    }, e -> {
                        log.error("", e);
                        heartFail(startVersion, null);
                    }, () -> {
                        if (heartSuccessNum.get() > nodeList.size() / 2) {
                            if ((heartSuccessNum.get() + closeNum.get()) == nodeList.size()
                                    && !state.state.equals(State.master)) {
                                heartSuccessNum.set(nodeList.size());
                            }
                            if (runningNodeSet.isEmpty() && state.state.equals(State.master)) {
                                runningInodeSet.clear();
                            }
                            heartSuccess(startVersion, heartSuccessNum.get(), nodeList);
                        } else {
                            if (voteNum.get() > nodeList.size() / 2 && remoteMaster.size() > 0) {
                                String voteMaster = getVoteMaster(remoteMaster, nodeList);
                                heartFail(startVersion, voteMaster);
                            } else {
                                heartFail(startVersion, null);
                            }
                        }
                    });
                }
            } catch (Exception e) {
                log.error("", e);
                heartFail(startVersion, null);
            }
        } else {
            state.writeLock.lock();
            try {
                if (state.state != State.down) {
                    heartFail(startVersion, null);
                }
            } finally {
                state.writeLock.unlock();
            }
        }
    }

    private void heartFail(long beforeVersion, String masterNode) {
        state.writeLock.lock();
        try {
            if (state.stateVersion == beforeVersion) {
                if (FS_VNODE_STATE_DEBUG) {
                    log.info("{} {} down, remoteMaster: {}", num, curNode, masterNode);
                }
                state.state = State.down;
                state.stateVersion++;
                lastHeartMap.clear();
                state.upNum = 0;
                state.heartTime = -1;
                state.masterNode = masterNode;
            }
        } finally {
            state.writeLock.unlock();
            executor.schedule(this::tryVote, HEART_MIL, TimeUnit.MILLISECONDS);
        }
    }

    public void heartFail0() {
        if (state.state.equals(State.slave)) {
            heartFail(state.stateVersion, null);
        }
    }

    private void heartSuccess(long beforeVersion, int successNum, List<Tuple3<String, String, String>> nodeList) {
        state.writeLock.lock();
        try {
            if (state.stateVersion == beforeVersion) {
                state.stateVersion++;

                if (state.state == State.slave) {
                    return;
                } else {
                    state.masterNode = curNode;
                    if (state.state == State.down || state.state == State.up) {
                        if (runningNodeSet.isEmpty() && (successNum == nodeList.size() || state.upNum >= UP_WAIT_NUM)) {
                            if (FS_VNODE_STATE_DEBUG) {
                                log.info("{} {}:{} master", num, curNode, getPriority(nodeList, RabbitMqUtils.CURRENT_IP));
                            }
                            VersionUtil.updateState(num);
                            // 清空缓存避免缓存已失效
                            inodeCache.cache.cache.clear();
                            state.state = State.master;
                        } else {
                            if (FS_VNODE_STATE_DEBUG) {
                                log.info("{} {}:{} up", num, curNode, getPriority(nodeList, RabbitMqUtils.CURRENT_IP));
                            }
                            state.state = State.up;
                            state.upNum++;
                        }
                    }
                }
            }

        } finally {
            state.writeLock.unlock();
            executor.schedule(this::tryVote, HEART_MIL, TimeUnit.MILLISECONDS);
        }

    }

    public String getHeart(long lastHeart, int priority, String node) {
        if (!enable.get()) {
            return "0";
        }
        state.writeLock.lock();
        try {
            if (curNode.equals(node)) {
                // 发给本节点的消息
                String masterNode = state.masterNode;
                List<Tuple3<String, String, String>> curList = Node.STORAGE_POOL.mapToNodeInfo(String.valueOf(storageVnode)).block();
                int priority0 = getPriority(curList, RabbitMqUtils.CURRENT_IP);

                if (priority0 == -1 || null != masterNode && (getMigrateCase(NodeCache.getIP(masterNode)) == 1)) {
                    if (FS_VNODE_STATE_DEBUG) {
                        log.info("{} {}:{}-{}:{} is migrate, res 0", num, node, priority, masterNode, priority0);
                    }
                    return "0";
                } else {
                    return "1";
                }
            } else if (lastHeart > 0 && lastHeart == Math.abs(state.heartTime)) {
                // 正常状态下主发给备的消息
                String masterNode = state.masterNode;
                if (null != masterNode && (getMigrateCase(NodeCache.getIP(masterNode)) == 1)) {
                    if (FS_VNODE_STATE_DEBUG) {
                        log.info("{} {}:{}-{} is migrate, {} {} doesn't update", num, node, priority, masterNode, curNode, state.heartTime);
                    }
                    return "0";
                } else {
                    state.stateVersion++;
                    state.heartTime = System.nanoTime();

                    if (inodeCache.running.isEmpty()) {
                        return Math.abs(state.heartTime) + "";
                    }
                    return -Math.abs(state.heartTime) + "#" + inodeCache.running.keySet().toString();
                }
            } else {
                // 选举过程中的消息
                long voteTime = System.nanoTime();
                List<Tuple3<String, String, String>> nodeList = Node.STORAGE_POOL.mapToNodeInfo(String.valueOf(storageVnode)).block();
                int priority0 = getPriority(nodeList, RabbitMqUtils.CURRENT_IP);

                // 检查发来请求的节点当前是否还在nodeList中，如果不在则返0；从而避免迁主down新加节点时出现masterNode为null的情况
                if (!isVnodeExist(nodeList, node)) {
                    if (FS_VNODE_STATE_DEBUG) {
                        log.info("{} {}:{} is out of list, {}:{} {} doesn't update", num, node, priority0, curNode, priority, voteTime);
                    }
                    //处理旧从节点还未完成重新选举的情况，直接返回0，代表心跳失败
                    if (StringUtils.isBlank(state.masterNode)) {
                        return "0";
                    }
                    return "0" + "@" + state.masterNode;
                }

                if (priority < priority0 || priority0 == -1) {
                    state.heartTime = voteTime;
                    state.masterNode = node;
                    state.state = State.slave;
                    state.stateVersion++;

                    if (FS_VNODE_STATE_DEBUG) {
                        log.info("{} {}:{} slave of {}:{} {}", num, curNode, priority0, node, priority, voteTime);
                    }
                    if (inodeCache.running.isEmpty()) {
                        return Math.abs(voteTime) + "";
                    } else {
                        return -Math.abs(voteTime) + "#" + inodeCache.running.keySet().toString();
                    }
                } else {
                    return -Math.abs(voteTime) + "";
                }
            }
        } finally {
            state.writeLock.unlock();
        }
    }

    public Mono<Inode> exec(InodeOperator operator) {
        state.readLock.lock();
        try {

            if (state.state == State.master
                    //vnode主切回之后，需等待前一个节点对inodeId相关的处理结束
                    && !runningInodeSet.contains(operator.nodeId)) {
                inodeCache.hook(operator);
                return operator.res;
            }

            return Mono.just(RETRY_INODE);
        } finally {
            state.readLock.unlock();
        }
    }
}

package com.macrosan.storage;

import com.macrosan.constants.ErrorNo;
import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.doubleActive.MsClientRequest;
import com.macrosan.ec.Utils;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.httpserver.ServerConfig;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.jsonmsg.PartInfo;
import com.macrosan.storage.codec.ErasureCodc;
import com.macrosan.storage.coder.*;
import com.macrosan.storage.coder.fs.LowPackageErasureEncoder;
import com.macrosan.storage.coder.fs.LowPackageReplicaEncoder;
import com.macrosan.storage.metaserver.BucketShardCache;
import com.macrosan.storage.metaserver.ShardingMapping;
import com.macrosan.storage.metaserver.statistic.ShardHotSpotStatistic;
import com.macrosan.utils.functional.Function5;
import com.macrosan.utils.functional.ImmutableTuple;
import com.macrosan.utils.functional.Tuple2;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsException;
import io.vertx.core.streams.ReadStream;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.storage.StoragePoolFactory.MAX_VNODE_NUM;
import static com.macrosan.storage.StoragePoolType.ERASURE;
import static com.macrosan.storage.StoragePoolType.REPLICA;
import static com.macrosan.storage.coder.ErasurePrefetchLimitEncoder.DISABLE_PREFETCH;

@Data
@Log4j2
public class StoragePool {
    private RedisConnPool pool = RedisConnPool.getInstance();

    private String vnodePrefix;
    private String mapVnodePrefix;
    private int k;
    private int m;
    private int vnodeNum;
    private int packageSize;
    private long divisionSize;
    private VnodeCache cache;
    private ShardingMapping shardingMapping;
    private PoolHealth.HealthState state;

    private Function<Long, Encoder> getEncoderFunction;
    private Function<ReadStream, LimitEncoder> getLimitEncoderFunction;
    private Function<ReadStream, LimitEncoder> getPerfetchLimitEncoderFunction;
    private Function5<Flux<byte[]>[], Long, Long, MsHttpRequest, MsClientRequest, Decoder> getDecoderFunction;

    private ErasureCodc codc;
    public StoragePoolType type;

    public String updateECPrefix;
    public String compression;
    Map<String, List<Tuple3<String, String, String>>> cachedNodeInfo;

    private volatile boolean stopHealthCheck;
    //0 HDD
    //1 sata ssd
    //2 nvme
    public int media = 0;

    StoragePool(String vnodePrefix, StoragePoolType type, int k, int m, int packageSize, long divisionSize,
                List<String> vnodeList, List<String> mapVnodeList, String updateECPrefix) {
        NodeCache.init();
        this.type = type;
        this.vnodePrefix = vnodePrefix;
        this.k = k;
        this.m = m;
        this.packageSize = packageSize;
        long minPut = (long) k * packageSize;
        this.divisionSize = divisionSize / minPut * minPut;

        this.updateECPrefix = updateECPrefix;

        vnodeNum = vnodeList.size();
        cachedNodeInfo = new ConcurrentHashMap<>(vnodeNum);
        cache = new VnodeCache(this, vnodeList, mapVnodeList);
        if (mapVnodeList != null) {
            mapVnodePrefix = StoragePoolFactory.getPrefix(mapVnodeList.get(0));
        }

        if (type == ERASURE) {
            codc = new ErasureCodc(k, m, packageSize);
            getEncoderFunction = size -> size > 0 && size < packageSize ? new LowPackageErasureEncoder(k, m, Math.toIntExact(size), codc) : new ErasureEncoder(k, m, packageSize, codc);
            getLimitEncoderFunction = r -> new ErasureLimitEncoder(k, m, packageSize, codc, r);
            getPerfetchLimitEncoderFunction = r -> new ErasurePrefetchLimitEncoder(k, m, packageSize, codc, r);
            getDecoderFunction = (flux, len, fileLen, request, clientRequest) -> new ErasureDecoder(k, m, packageSize, codc, flux, len, request, clientRequest);
        } else {
            getEncoderFunction = size -> size > 0 && size < packageSize ? new LowPackageReplicaEncoder(k, m, Math.toIntExact(size)) : new ReplicaEncoder(k, m, packageSize);
            getLimitEncoderFunction = r -> new ReplicaLimitEncoder(k, m, packageSize, r);
            getPerfetchLimitEncoderFunction = r -> new ReplicaPrefetchLimitEncoder(k, m, packageSize, r);
            getDecoderFunction = (flux, len, fileLen, request, clientRequest) -> new ReplicaDecoder(k, m, flux, fileLen, request, clientRequest);
        }

        if (vnodePrefix.startsWith("meta")) {
            shardingMapping = new ShardingMapping(cache, vnodePrefix, mapVnodePrefix);
            ShardHotSpotStatistic.updateVNodeUsageBytes(this);
        }

        PoolHealth.updateHealth(this);

        if (!cache.lunSet.isEmpty()) {
            String lun = cache.lunSet.iterator().next();
            Map<String, String> map = RedisConnPool.getInstance().getCommand(REDIS_LUNINFO_INDEX).hgetall(lun);
            String mediaStr = map.get("media");
            String protocol = map.get("protocol");
            boolean ssd = "SSD".equalsIgnoreCase(mediaStr);
            boolean isNvme = "NVME".equalsIgnoreCase(protocol);
            if (ssd) {
                if (isNvme) {
                    media = 2;
                } else {
                    media = 1;
                }
            } else {
                media = 0;
            }
        }
    }

    StoragePool(String vnodePrefix, StoragePoolType type, int k, int m, int packageSize, long divisionSize, List<String> vnodeList,
                List<String> mapVnodeList, String updateECPrefix, String compression) {
        this(vnodePrefix, type, k, m, packageSize, divisionSize, vnodeList, mapVnodeList, updateECPrefix);
        this.compression = compression;
    }

    public List<String> getLocalLun(String vnode) {
        List<String> res = new ArrayList<>();
        if (!vnode.startsWith(vnodePrefix) && !vnode.startsWith(mapVnodePrefix)) {
            vnode = vnodePrefix + vnode;
        }
        String link = cache.hget(vnode, "link");
        String[] linkArray = link.substring(1, link.length() - 1).split(",");
        for (String linkVNode : linkArray) {
            linkVNode = vnodePrefix + linkVNode;
            String sUuid = cache.hget(linkVNode, "s_uuid");
            if (ServerConfig.getInstance().getHostUuid().equals(sUuid)) {
                String localLun = cache.hget(linkVNode, "lun_name");
                if (StringUtils.isNotBlank(localLun)) {
                    res.add(localLun);
                }
            }
        }
        return res;
    }

    void removeNodeInfoCache(String vnode) {
        if (vnode.charAt(0) == '-') {
            vnode = reverseMapVnode(vnode).substring(vnodePrefix.length());
        }

        int n = Integer.parseInt(vnode);

        if (n >= vnodeNum) {
            n = n % vnodeNum;
            vnode = String.valueOf(n);
        }

        vnode = vnodePrefix + vnode;
        String prefix;
        String mapV = cache.hget(vnode, "map");
        if (mapV != null) {
            vnode = mapV;
            prefix = mapVnodePrefix;
        } else {
            prefix = vnodePrefix;
        }

        String linkStr = cache.hgetVnodeInfo(vnode, "link").block();
        String[] link = linkStr.substring(1, linkStr.length() - 1).split(",");
        Set<Integer> poolLinkSet = Stream.of(link).map(Integer::parseInt).collect(Collectors.toSet());
        if (MAX_VNODE_NUM > vnodeNum) {
            Set<Integer> next = new HashSet<>();

            do {
                next.clear();
                for (int v0 : poolLinkSet) {
                    if (v0 + vnodeNum < MAX_VNODE_NUM && !poolLinkSet.contains(v0 + vnodeNum)) {
                        next.add(v0 + vnodeNum);
                    }
                }

                poolLinkSet.addAll(next);
            } while (!next.isEmpty());
        }
        link = poolLinkSet.stream().map(String::valueOf).toArray(String[]::new);
        for (String v : link) {
//            String vv = prefix + v;//无需前缀
            cachedNodeInfo.compute(v, (k, v0) -> {
                return null;
            });
        }
    }

    //需要修改nodeList的使用mapToNodeInfoW获得
    public Mono<List<Tuple3<String, String, String>>> mapToNodeInfoW(String vnode) {
        return mapToNodeInfo(vnode).map(nodeList -> {
            ArrayList<Tuple3<String, String, String>> copy = new ArrayList<>(nodeList.size());
            for (Tuple3<String, String, String> t : nodeList) {
                copy.add(new Tuple3<>(t.var1, t.var2, t.var3));
            }
            return copy;
        });
    }

    private static ArrayList<Tuple3<String, String, String>> cloneNodeList(List<Tuple3<String, String, String>> cached) {
        ArrayList<Tuple3<String, String, String>> copy = new ArrayList<>(cached.size());
        for (Tuple3<String, String, String> t : cached) {
            copy.add(new Tuple3<>(t.var1, t.var2, t.var3));
        }
        return copy;
    }

    public Mono<List<Tuple3<String, String, String>>> mapToNodeInfo(String vnode) {
        List<Tuple3<String, String, String>> cached = cachedNodeInfo.get(vnode);
        if (cached == null) {
            cached = cachedNodeInfo.computeIfAbsent(vnode, k -> {
                return mapToNodeInfo0(vnode).block();
            });
        }

        return Mono.just(cloneNodeList(cached));//返回拷贝后的nodeList避免后续处理修改缓存
    }

    public Mono<List<Tuple3<String, String, String>>> mapToNodeInfo0(String vnode) {
        if (vnode.charAt(0) == '-') {
            vnode = reverseMapVnode(vnode).substring(vnodePrefix.length());
        }

        int n = Integer.parseInt(vnode);

        if (n >= vnodeNum) {
            n = n % vnodeNum;
            vnode = String.valueOf(n);
        }

        vnode = vnodePrefix + vnode;
        String prefix;
        String mapV = cache.hget(vnode, "map");
        if (mapV != null) {
            vnode = mapV;
            prefix = mapVnodePrefix;
        } else {
            prefix = vnodePrefix;
        }

        return cache.hgetVnodeInfo(vnode, "link")
                .map(link -> link.substring(1, link.length() - 1).split(","))
                .flatMapMany(Flux::fromArray)
                .index()
                .map(t -> {
                    Map<String, String> vnodeInfo = cache.getVnodeInfo(prefix + t.getT2()).block();
                    String ip = NodeCache.getIP(vnodeInfo.get("s_uuid"));
                    String curV = vnodeInfo.get("v_num");
                    if (mapV != null) {
                        if (StringUtils.isNotBlank(vnodeInfo.get("reverseMap"))) {
                            curV = vnodeInfo.get("reverseMap").substring(vnodePrefix.length());
                        } else {
                            curV = "-" + curV;
                        }
                    }

                    return Tuples.of(t.getT1(), new Tuple3<>(ip, vnodeInfo.get(VNODE_LUN_NAME), curV));
                })
                .collectMap(reactor.util.function.Tuple2::getT1)
                .map(resMap -> {
                    List<Tuple3<String, String, String>> res = new ArrayList<>(k + m);
                    for (long i = 0; i < k + m; i++) {
                        res.add(resMap.get(i).getT2());
                    }
                    return res;
                });
    }

    public String getBucketVnodeId(String bucketName) {
        return MsVnodeUtils.getTargetVnodeId(bucketName, vnodeNum);
    }

    public String getBucketVnodeId(String bucketName, String objectName) {
        return bucketShardCache.get(bucketName).find(objectName);
    }

    public Tuple2<String, String> getBucketVnodeIdTuple(String bucketName, String objectName) {
        return bucketShardCache.get(bucketName).findMigrate(objectName);
    }

    public List<String> getBucketVnodeList(String bucketName) {
        return bucketShardCache.get(bucketName).getAllLeafNode();
    }

    public ImmutableTuple<String, String> getObjectVnodeId(String bucketName, String objectName) {
        return MsVnodeUtils.getObjectVnodeId(bucketName, objectName, vnodeNum);
    }

    public String getObjectVnodeId(MetaData metaData) {
        if (metaData.getPartInfos() != null) {
            //处理文件数据为holeFile，fileName为""的情况
            if (null != metaData.getPartInfos() && StringUtils.isBlank(metaData.partInfos[0].fileName)) {
                String tempFileName = null;
                for (int i = 0; i < metaData.partInfos.length; i++) {
                    if (StringUtils.isNotBlank(metaData.partInfos[i].fileName)) {
                        tempFileName = metaData.partInfos[i].fileName;
                    }
                }

                if (StringUtils.isBlank(tempFileName)) {
                    StorageOperate operate = new StorageOperate(StorageOperate.PoolType.DATA, "", Long.MAX_VALUE);
                    StoragePool dataPool = StoragePoolFactory.getStoragePool(operate, metaData.bucket);
                    tempFileName = Utils.getObjFileName(dataPool, metaData.bucket, metaData.getKey() + RandomStringUtils.randomAlphanumeric(4),
                            RandomStringUtils.randomAlphanumeric(32));
                }

                return tempFileName.split(File.separator)[1].split("_")[0];
            }

            return metaData.partInfos[0].fileName.split(File.separator)[1].split("_")[0];
        } else {
            return metaData.fileName.split(File.separator)[1].split("_")[0];
        }
    }

    public String getObjectVnodeId(String fileName) {
        return fileName.split(File.separator)[1].split("_")[0];
    }

    public String getObjectVnodeId(PartInfo partInfo) {
        return partInfo.fileName.split(File.separator)[1].split("_")[0];
    }

    public String getDedupVnode(String dedupliacate) {
        return dedupliacate.split(File.separator)[0].substring(1);
    }

    public boolean contains(String lun) {
        return cache.lunSet.contains(ServerConfig.getInstance().getHostUuid() + '@' + lun);
    }

    public void addLun(String uuid, String lun) {
        cache.lunSet.add(uuid + '@' + lun);
    }

    public void updateVnodeDisk(String vnode, String newDisk) {
        String vnodeNum = vnode;//不带前缀
        if (vnode.charAt(0) == '-') {
            vnode = mapVnodePrefix + vnode.substring(1);
            cache.updateVnodeDisk(vnode, newDisk, vnodeNum);
            log.info("update cache {} {}", vnode, newDisk);
        } else {
            vnode = vnodePrefix + vnode;
            cache.updateVnodeDisk(vnode, newDisk, vnodeNum);
            String mapV = cache.hget(vnode, "map");
            if (mapV != null) {
                cache.updateVnodeDisk(mapV, newDisk, vnodeNum);
                log.info("update cache {} {} {}", vnode, mapV, newDisk);
            } else {
                log.info("update cache {} {}", vnode, newDisk);
            }
        }
    }

    public void updateVnodeNode(String vnode, String node, String disk) {
        String vnodeNum = vnode;//不带前缀
        if (vnode.charAt(0) == '-') {
            vnode = mapVnodePrefix + vnode.substring(1);
            cache.updateVnodeNode(vnode, node, disk, vnodeNum);
            log.info("update cache {} {} {} ", vnode, node, disk);
        } else {
            vnode = vnodePrefix + vnode;
            cache.updateVnodeNode(vnode, node, disk, vnodeNum);
            String mapV = cache.hget(vnode, "map");
            if (mapV != null) {
                cache.updateVnodeNode(vnode, node, disk, vnodeNum);
                log.info("update cache {} {} {} {} ", vnode, mapV, node, disk);
            } else {
                log.info("update cache {} {} {} ", vnode, node, disk);
            }
        }
    }

    public Encoder getEncoder() {
        return getEncoderFunction.apply(-1L);
    }

    public Encoder getEncoder(long size) {
        return getEncoderFunction.apply(size);
    }

    public LimitEncoder getLimitEncoder(ReadStream request) {
        return getLimitEncoder(request, false);
    }

    public LimitEncoder getLimitEncoder(ReadStream request, boolean prefetch) {
        if (prefetch && !DISABLE_PREFETCH) {
            return getPerfetchLimitEncoderFunction.apply(request);
        }
        return getLimitEncoderFunction.apply(request);
    }

    public Decoder getDecoder(Flux<byte[]>[] dataFluxes, long objectLen, long fileLen, MsHttpRequest request, MsClientRequest clientRequest) {
        return getDecoderFunction.apply(dataFluxes, objectLen, fileLen, request, clientRequest);
    }

    public long getOriginalObjSize(long fileSize) {
        if (type == REPLICA) {
            return fileSize;
        }

        if (type == ERASURE) {
            long objSize = 0L;
            while (fileSize >= packageSize) {
                objSize += packageSize * k;
                fileSize -= packageSize;
            }

            if (fileSize != 0) {
                objSize += fileSize * k - 1;
            }

            return objSize;
        }

        return -1;
    }

    private String reverseMapVnode(String vnode) {
        String key = mapVnodePrefix + vnode.substring(1);

        if (cache.hget(key, "reverseMap") != null) {
            return cache.hget(key, "reverseMap");
        }

        String link = cache.hget(key, "link");
        for (String mapV : link.substring(1, link.length() - 1).split(",")) {
            String reverseV = cache.hget(mapVnodePrefix + mapV, "reverseMap");
            if (reverseV != null) {
                return reverseV;
            }
        }

        throw new MsException(ErrorNo.UNKNOWN_ERROR, "reverseMapVnode fail " + vnode);
    }


    public String[] getLink(String vnode) {
        if (vnode.charAt(0) == '-') {
            vnode = reverseMapVnode(vnode);
        }

        if (vnode.startsWith(vnodePrefix)) {
            vnode = vnode.substring(vnodePrefix.length());
        }

        int v = Integer.parseInt(vnode);
        if (v >= vnodeNum) {
            v = v % vnodeNum;
            vnode = String.valueOf(v);
        }

        Set<Integer> poolLink = getLinkSet(vnode).stream().map(Integer::parseInt).collect(Collectors.toSet());


        if (MAX_VNODE_NUM > vnodeNum) {
            Set<Integer> next = new HashSet<>();

            do {
                next.clear();
                for (int v0 : poolLink) {
                    if (v0 + vnodeNum < MAX_VNODE_NUM && !poolLink.contains(v0 + vnodeNum)) {
                        next.add(v0 + vnodeNum);
                    }
                }

                poolLink.addAll(next);
            } while (!next.isEmpty());
        }

        return poolLink.stream().map(String::valueOf).toArray(String[]::new);
    }

    private Set<String> getLinkSet(String vnode) {
        String[] linkVNodes = getLinkVNodes(vnode);
        Set<String> link = new HashSet<>(Arrays.asList(linkVNodes));

        int curLinkSize;
        int nextLinkSize;
        do {
            curLinkSize = link.size();
            Set<String> curLink = new HashSet<>(link);
            for (String v : curLink) {
                link.addAll(Arrays.asList(getLinkVNodes(v)));
            }

            nextLinkSize = link.size();
        } while (nextLinkSize != curLinkSize);

        return link;
    }

    private String[] getLinkVNodes(String vnode) {
        vnode = vnodePrefix + vnode;

        return cache.hgetVnodeInfo(vnode, "link")
                .map(link -> link.substring(1, link.length() - 1).split(","))
                .block();
    }

    private static BucketShardCache bucketShardCache;

    public static void loadBucketShard() {
        if (bucketShardCache == null) {
            bucketShardCache = BucketShardCache.getInstance();
        }
    }

    public BucketShardCache getBucketShardCache() {
        loadBucketShard();
        return bucketShardCache;
    }
}

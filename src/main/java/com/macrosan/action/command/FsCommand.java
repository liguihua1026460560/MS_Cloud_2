package com.macrosan.action.command;

import com.macrosan.database.redis.RedisConnPool;
import com.macrosan.ec.DelFileRunner;
import com.macrosan.ec.error.DiskErrorHandler;
import com.macrosan.ec.server.WriteCacheServer;
import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.cache.*;
import com.macrosan.filesystem.cifs.CIFS;
import com.macrosan.filesystem.cifs.handler.SMBHandler;
import com.macrosan.filesystem.cifs.lease.LeaseServer;
import com.macrosan.filesystem.cifs.lock.CIFSLockServer;
import com.macrosan.filesystem.cifs.shareAccess.ShareAccessServer;
import com.macrosan.filesystem.ftp.FTP;
import com.macrosan.filesystem.lock.redlock.RedLockClient;
import com.macrosan.filesystem.lock.redlock.RedLockServer;
import com.macrosan.filesystem.nfs.NFS;
import com.macrosan.filesystem.nfs.api.NFS3Proc;
import com.macrosan.filesystem.nfs.api.NFS4Proc;
import com.macrosan.filesystem.nfs.lock.NLMLockClient;
import com.macrosan.filesystem.nfs.lock.NLMLockServer;
import com.macrosan.filesystem.tier.FileTierMove;
import com.macrosan.filesystem.utils.FsTierUtils;
import com.macrosan.filesystem.utils.IpWhitelistUtils;
import com.macrosan.filesystem.utils.ReadDirCache;
import com.macrosan.filesystem.utils.RunNumUtils;
import com.macrosan.filesystem.utils.acl.ACLUtils;
import com.macrosan.filesystem.utils.acl.CIFSACL;
import com.macrosan.httpserver.RestfulVerticle;
import com.macrosan.localtransport.Command;
import com.macrosan.message.jsonmsg.CliResponse;
import com.macrosan.message.jsonmsg.FSQuotaConfig;
import com.macrosan.storage.move.CacheMove;
import com.macrosan.utils.functional.Tuple3;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import static com.macrosan.constants.SysConstants.REDIS_SYSINFO_INDEX;
import static com.macrosan.filesystem.FsConstants.SMB2ACCESS_MASK.*;
import static com.macrosan.filesystem.FsConstants.SMB2ACEFlag.*;
import static com.macrosan.filesystem.cache.Vnode.FS_VNODE_STATE_DEBUG;
import static com.macrosan.filesystem.lock.Lock.*;
import static com.macrosan.filesystem.quota.FSQuotaRealService.getQuotaConfigCache;

@Log4j2
public class FsCommand extends Reusable {
    private static final FsCommand instance = new FsCommand();

    public static FsCommand getInstance() {
        return instance;
    }

    @Command
    public CliResponse tune(String[] args) {
        log.info("mossfs action: " + Arrays.toString(args));
        try {
            String action = args[0];
            switch (action) {
                case "print":
                    int vNum = Integer.parseInt(args[1]);
                    boolean isInode = Boolean.parseBoolean(args[2]);
                    int printNum = Integer.parseInt(args[3]);
                    Node.getInstance().getVnode(vNum).getInodeCache().getCache().print(isInode, printNum);
                    break;
                case "find":
                    long nodeId = Long.parseLong(args[1]);
                    int index = Math.abs((int) (nodeId % 4096));
                    Node.getInstance().getVnode(index).getInodeCache().getCache().find(nodeId);
                    break;
                case "state":
                    long nodeId1 = Long.parseLong(args[1]);
                    int index1 = Math.abs((int) (nodeId1 % 4096));
                    log.info(Node.getInstance().getVnode(index1).getVnodeState());
                    break;
                case "proc":
                    boolean procDebug = Boolean.parseBoolean(args[1]);
                    NFS3Proc.debug = procDebug;
                    break;
                case "listDebug":
                    boolean listDebug = Boolean.parseBoolean(args[1]);
                    ReadDirCache.debug = listDebug;
                    break;
                case "rebuild":
                    boolean rebuildDebug = Boolean.parseBoolean(args[1]);
                    DiskErrorHandler.NFS_REBUILD_DEBUG = rebuildDebug;
                    break;
                case "cifsDebug":
                    boolean cifsDebug = Boolean.parseBoolean(args[1]);
                    CIFS.cifsDebug = cifsDebug;
                    break;
                case "nfsDebug":
                    boolean nfsDebug = Boolean.parseBoolean(args[1]);
                    NFS.nfsDebug = nfsDebug;
                    break;
                case "runningDebug":
                    boolean runningDebug = Boolean.parseBoolean(args[1]);
                    SMBHandler.runningDebug = runningDebug;
                    break;
                case "lockDebug":
                    boolean lockDebug = Boolean.parseBoolean(args[1]);
                    RedLockClient.LOCK_DEBUG = lockDebug;
                    break;
                case "lockEnable":
                    boolean lockEnable = Boolean.parseBoolean(args[1]);
                    RedLockClient.LOCK_ENABLE = lockEnable;
                    break;
                case "NLMDebug":
                    boolean NLMDebug = Boolean.parseBoolean(args[1]);
                    NLMLockClient.LOCK_DEBUG = NLMDebug;
                    break;
                case "NLMLockInfo":
                    log.info("【NLMLockInfo:】" + NLMLockServer.lockInfo());
                    break;
                case "lockInfo":
                    int lockType = Integer.parseInt(args[1]);
                    switch (lockType) {
                        case RED_LOCK_TYPE:
                            log.info("【RED lockInfo:】" + RedLockServer.info());
                            break;
                        case LEASE_TYPE:
                            log.info("【CIFS Lease lockInfo:】" + LeaseServer.info());
                            break;
                        case SHARE_ACCESS_TYPE:
                            log.info("【CIFS ShareAccess lockInfo:】" + ShareAccessServer.info());
                            break;
                        case CIFS_LOCK_TYPE:
                            log.info("【CIFS Lock lockInfo:】" + CIFSLockServer.info());
                    }
                    break;
                case "sessionInfo":
                    log.info("【sessionInfo:】" + SMBHandler.info());
                    break;
                case "writeCacheDebug":
                    try {
                        WriteCacheServer.writeCacheDebug = Boolean.parseBoolean(args[1]);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    log.info("【writeCacheDebug:】" + WriteCacheServer.writeCacheDebug);
                    break;
                case "printSMBStack":
                    boolean notPrint = Boolean.parseBoolean(args[1]);
                    SMBHandler.notPrintStackDebug = notPrint;
                    break;
                case "getAvailableCache":
                    CliResponse cacheNumResponse = new CliResponse();
                    long availableCache = WriteCache.getAvailableCacheNum();
                    log.info("【availableCache】num:{}", availableCache);
                    cacheNumResponse.setStatus("availableCache num:" + availableCache);
                    return cacheNumResponse;
                case "isFlushing":
                    CliResponse isFlushing = new CliResponse();
                    boolean flushing = WriteCache.getFlushing();
                    log.info("【isFlushing】:{}", flushing);
                    isFlushing.setStatus("isFlushing:" + flushing);
                    return isFlushing;
                case "getRunning":
                    CliResponse cliResponse = new CliResponse();
                    long running = RunNumUtils.getRunNum().get();
                    log.info("【running】num:{}", running);
                    cliResponse.setStatus("running num:" + running);
                    return cliResponse;
                case "getInodeTimeMap":
                    CliResponse getInodeTimeMap = new CliResponse();
                    log.info("【getInodeTimeMap】{}", Node.INODE_TIME_MAP.toString());
                    getInodeTimeMap.setStatus("【getInodeTimeMap】:{}" + Node.INODE_TIME_MAP.toString());
                    return getInodeTimeMap;
                case "heartDebug":
                    boolean heartDebug = Boolean.parseBoolean(args[1]);
                    FS_VNODE_STATE_DEBUG = heartDebug;
                    break;
                case "vNum":
                    vNum(args);
                    break;
                case "AdjustRead":
                    AdjustRead(args);
                    break;
                case "ftpDebug":
                    FTP.ftpDebug = Boolean.parseBoolean(args[1]);
                    break;
                case "printAll":
                    printAll();
                    break;
                case "cacheSwitch":
                    CacheMove.cacheSwitch = Boolean.parseBoolean(args[1]);
                    if (CacheMove.cacheSwitch) {
                        log.info("Cache move on");
                    } else {
                        log.info("Cache move off");
                    }
                    break;
                case "getQuotaCache":
                    String bucketName = args[1];
                    Map<String, FSQuotaConfig> quotaConfigCache = getQuotaConfigCache(bucketName);
                    log.info("【getQuotaCache】bucketName:{}, quotaConfigCache:{}", bucketName, quotaConfigCache);
                    CliResponse quotaCacheResponse = new CliResponse();
                    quotaCacheResponse.setStatus("quotaConfigCache:" + quotaConfigCache);
                    return quotaCacheResponse;
                case "aclDebug":
                    boolean aclDebug = Boolean.parseBoolean(args[1]);
                    ACLUtils.aclDebug = aclDebug;
                    break;
                case "initId":
                    ACLUtils.init();
                    log.info("load s3IDtoGids: {}", ACLUtils.s3IDToGids);
                    break;
                case "printWriteCache":
                    WriteCacheNode.printWriteCache();
                    break;
                case "initWhiteList":
                    IpWhitelistUtils.init();
                    log.info("load initWhiteList: {}", IpWhitelistUtils.nfsIpWhitelistMap);
                    break;
                case "printInfo":
                    return printInfo(args);
                case "aclStart":
                    boolean aclStart = Boolean.parseBoolean(args[1]);
                    ACLUtils.NFS_ACL_START = aclStart;
                    break;
                case "updateFsPort":
                    CliResponse updateFsPortRes = new CliResponse();
                    try {
                        int res = updateFsPort();
                        updateFsPortRes.setStatus(String.valueOf(res));
                    } catch (Exception e) {
                        log.error("update fs port error, ", e);
                        updateFsPortRes.setStatus("-1");
                    }
                    return updateFsPortRes;
                case "updateFsId":
                    String msg = args[1];
                    CliResponse updateRes = new CliResponse();
                    try {
                        int res = ACLUtils.updateFsIdentiy(msg);
                        updateRes.setStatus(String.valueOf(res));
                    } catch (Exception e) {
                        log.error("updateFsId error, msg: {}, ", msg, e);
                        updateRes.setStatus("-1");
                    }
                    return updateRes;
                case "notifyAclStart":
                    CliResponse startRes = new CliResponse();
                    try {
                        boolean start = Boolean.parseBoolean(args[1]);
                        if (args.length == 2) {
                            int res = ACLUtils.switchNFSACL(start, "nfs");
                            startRes.setStatus(String.valueOf(res));
                        } else if (args.length == 3) {
                            String protoType = args[2];
                            if (!protoType.equals("nfs") && !protoType.equals("cifs")) {
                                startRes.setStatus("-1");
                            } else {
                                int res = ACLUtils.switchNFSACL(start, protoType);
                                startRes.setStatus(String.valueOf(res));
                            }
                        }
                    } catch (Exception e) {
                        log.error("switch nfs acl error, ", e);
                        startRes.setStatus("-1");
                    }
                    return startRes;
                case "notifyCifsAclStart":
                    CliResponse startCifsRes = new CliResponse();
                    try {
                        boolean start = Boolean.parseBoolean(args[1]);
                        int res = ACLUtils.switchNFSACL(start, "cifs");
                        startCifsRes.setStatus(String.valueOf(res));
                    } catch (Exception e) {
                        log.error("switch cifs acl error, ", e);
                        startCifsRes.setStatus("-1");
                    }
                    return startCifsRes;
                case "updateAllFsIdentity":
                    CliResponse updateAllRes = new CliResponse();
                    try {
                        String updateAction = args[1];
                        String res = ACLUtils.updateAllFsIdentityCommand(updateAction);
                        updateAllRes.setStatus(res);
                    } catch (Exception e) {
                        log.error("updateAllFsIdentity error, ", e);
                        updateAllRes.setStatus("fail");
                    }
                    return updateAllRes;
                case "adjustIdRange":
                    CliResponse adjustRes = new CliResponse();
                    try {
                        String maxIdStr = args[1];
                        int maxId = Integer.parseInt(maxIdStr);
                        int res = ACLUtils.adjustMaxId(maxId);
                        adjustRes.setStatus(String.valueOf(res));
                    } catch (Exception e) {
                        log.error("adjust id range, ", e);
                        adjustRes.setStatus("-1");
                    }
                    return adjustRes;
                case "printNFSIpWhitelists":
                    CliResponse printNFSIpWhitelistsRes = new CliResponse();
                    try {
                        String bucket = args[1];
                        printNFSIpWhitelistsRes.setStatus("【" + bucket + "】 【NFSIpWhitelists】:" + IpWhitelistUtils.nfsIpWhitelistMap.get(bucket));
                    } catch (Exception e) {
                        log.error("printNFSIpWhitelists error, ", e);
                        printNFSIpWhitelistsRes.setStatus("printNFSIpWhitelists error");
                    }
                    return printNFSIpWhitelistsRes;
                case "updateNFSIpWhitelists":
                    CliResponse updateNFSIpWhitelistsRes = new CliResponse();
                    try {
                        String bucket = args[1];
                        String res = IpWhitelistUtils.updateNFSIpWhitelistsCommand(bucket);
                        updateNFSIpWhitelistsRes.setStatus(res);
                    } catch (Exception e) {
                        log.error("updateNFSIpWhitelists error, ", e);
                        updateNFSIpWhitelistsRes.setStatus("fail");
                    }
                    return updateNFSIpWhitelistsRes;
                case "cifsAclDebug":
                    boolean cifsAclDebug = Boolean.parseBoolean(args[1]);
                    CIFSACL.cifsACL = cifsAclDebug;
                    break;
                case "parseNfsMode":
                    CliResponse parseNfsRes = new CliResponse();
                    String nfsModeRes = parseNfsMode(args[1]);
                    parseNfsRes.setStatus("parse res:" + nfsModeRes);
                    return parseNfsRes;
                case "parseCifsAccess":
                    CliResponse parseRes = new CliResponse();
                    String res = parseCifsAccess(args);
                    parseRes.setStatus("parse res:" + res);
                    return parseRes;
                case "parseCifsInheritFlag":
                    CliResponse parseFlagRes = new CliResponse();
                    String flagRes = parseCifsInherit(args);
                    parseFlagRes.setStatus("parse res:" + flagRes);
                    return parseFlagRes;
                case "usefulCifsAcl":
                    CliResponse printRes = new CliResponse();
                    String priRes = usefulCifsACL();
                    printRes.setStatus("print res:" + priRes);
                    return printRes;
                case "calCifsRight":
                    CliResponse calRes = new CliResponse();
                    String calStrRes = calCifsRight(args);
                    calRes.setStatus("print res:" + calStrRes);
                    return calRes;
                case "s3Running":
                    CliResponse s3Running = new CliResponse();
                    if (args.length > 1) {
                        synchronized (RestfulVerticle.class) {
                            int n = Integer.parseInt(args[1]);
                            if (n < 0) {
                                n = 100000;
                            }

                            if (n > RestfulVerticle.MAX_S3_RUNNING) {
                                RestfulVerticle.token.release(n - RestfulVerticle.MAX_S3_RUNNING);
                            } else if (n < RestfulVerticle.MAX_S3_RUNNING) {
                                Method method = Semaphore.class.getDeclaredMethod("reducePermits", int.class);
                                method.setAccessible(true);
                                method.invoke(RestfulVerticle.token, RestfulVerticle.MAX_S3_RUNNING - n);
                            }

                            RestfulVerticle.MAX_S3_RUNNING = n;
                        }
                    } else {
                        int max = RestfulVerticle.MAX_S3_RUNNING;
                        log.info("s3 running:{} max:{}", max - RestfulVerticle.token.availablePermits(), max);
                    }
                    return s3Running;
                case "readObjCache":
                    boolean readObjCache = Boolean.parseBoolean(args[1]);
                    ReadObjCache.readCacheDebug = readObjCache;
                    break;
                case "localIpDebug":
                    boolean localIpDebug = Boolean.parseBoolean(args[1]);
                    SMBHandler.localIpDebug = localIpDebug;
                    break;
                case "nfs4ErrorDebug":
                    boolean nfs4Debug = Boolean.parseBoolean(args[1]);
                    NFS4Proc.errorDebug = nfs4Debug;
                    break;
                case "fsTierDebug":
                    FsTierUtils.FS_TIER_DEBUG = Boolean.parseBoolean(args[1]);
                    break;
                case "fsTierCacheSwitch":
                    FileTierMove.cacheSwitch = Boolean.parseBoolean(args[1]);
                    break;
                case "deleteFileRun":
                    String opt;
                    if (args.length > 1) {
                        opt = args[1];
                    } else {
                        opt = "";
                    }

                    switch (opt) {
                        //set limit
                        case "hdd": {
                            DelFileRunner.HDD_LIMIT = Integer.parseInt(args[2]);
                            break;
                        }
                        case "ssd": {
                            DelFileRunner.SSD_LIMIT = Integer.parseInt(args[2]);
                        }
                        case "nvme": {
                            DelFileRunner.NVME_LIMIT = Integer.parseInt(args[2]);
                        }
                        case "reload": {
                            String hdd = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hget("delete_file_limit", "hdd");
                            if (StringUtils.isNotBlank(hdd)) {
                                DelFileRunner.HDD_LIMIT = Integer.parseInt(hdd);
                            }

                            String ssd = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hget("delete_file_limit", "ssd");
                            if (StringUtils.isNotBlank(ssd)) {
                                DelFileRunner.SSD_LIMIT = Integer.parseInt(ssd);
                            }

                            String nvme = RedisConnPool.getInstance().getCommand(REDIS_SYSINFO_INDEX).hget("delete_file_limit", "nvme");
                            if (StringUtils.isNotBlank(nvme)) {
                                DelFileRunner.NVME_LIMIT = Integer.parseInt(nvme);
                            }
                        }
                        //set running
                        case "running": {
                            String storage = args[2];
                            int num = Integer.parseInt(args[3]);
                            DelFileRunner.deleteRunning.get(storage).addAndGet(num);
                            break;
                        }
                        case "debug": {
                            DelFileRunner.DEBUG = Boolean.parseBoolean(args[2]);
                            break;
                        }
                        default:
                            log.info("running {}", DelFileRunner.deleteRunning);
                            log.info("limit {}", DelFileRunner.deleteLimit);
                    }
                    break;
                default:
            }
        } catch (Exception e) {
            log.error("", e);
        }

        return response();
    }

    public static void vNum(String[] args) {
        try {
            int vnodeNum = Integer.parseInt(args[1]);
            Class clazz = Class.forName("com.macrosan.filesystem.cache.Vnode");
            Field field_vnodeState = clazz.getDeclaredField("state");
            field_vnodeState.setAccessible(true);

            Class clazz2 = Class.forName("com.macrosan.filesystem.cache.Vnode$VnodeState");
            Field field_masterNode = clazz2.getDeclaredField("masterNode");
            field_masterNode.setAccessible(true);

            Vnode vnode = Node.getInstance().getVnode(vnodeNum);
            Object state = field_vnodeState.get(vnode);
            String vState = vnode.getVnodeState();
            String node = (String) field_masterNode.get(state);

            double multi = Node.STORAGE_POOL.getVnodeNum() * 1.0 / Node.TOTAL_V_NUM;
            int storageVnode = (int) (vnodeNum * multi);
            List<Tuple3<String, String, String>> list = Node.STORAGE_POOL.mapToNodeInfo(String.valueOf(storageVnode)).block();
            List<Tuple3<String, String, String>> cachedList = vnode.getCachedNodeList();

            log.info("vnode: {}, state: {}, master: {}, storageVnode: {}, curList: {}, cachedList: {}", vnodeNum, vState, node, storageVnode, list, cachedList);
        } catch (Exception e) {
            log.error("", e);
        }
    }

    public static void AdjustRead(String[] args) {
        try {
            String action = args[1];
            switch (action) {
                case "minPrefetch":
                    int min = Integer.parseInt(args[2]);
                    InodeCache.minPrefetch = min;
                    break;
                case "maxPrefetch":
                    int max = Integer.parseInt(args[2]);
                    InodeCache.maxPrefetch = max;
                    break;
                case "minInodeSize":
                    long minSize = Long.parseLong(args[2]);
                    InodeCache.MIN_INODE_SIZE = minSize;
                    break;
                case "maxInodeSize":
                    long maxSize = Long.parseLong(args[2]);
                    InodeCache.MAX_INODE_SIZE = maxSize;
                    break;
            }
        } catch (Exception e) {
            log.error("adjust read param error", e);
        }
    }

    public static void printAll() {
        try {
            Class clazz = Class.forName("com.macrosan.filesystem.cache.Vnode");
            Field field_vnodeState = clazz.getDeclaredField("state");
            field_vnodeState.setAccessible(true);

            Class clazz2 = Class.forName("com.macrosan.filesystem.cache.Vnode$VnodeState");
            Field field_masterNode = clazz2.getDeclaredField("masterNode");
            field_masterNode.setAccessible(true);

            for (int i = 0; i < 4096; i++) {
                Vnode vnode = Node.getInstance().getVnode(i);
                Object state = field_vnodeState.get(vnode);
                String vState = vnode.getVnodeState();
                String node = (String) field_masterNode.get(state);
                log.info("v: {}, state: {}, master: {}", i, vState, node);
            }
        } catch (Exception e) {
            log.error("", e);
        }
    }

    public static String parseNfsMode(String mode) {
        String res = "";
        try {
            res = Integer.toOctalString(Integer.parseInt(mode) & 4095);
        } catch (Exception e) {
            log.error("parse nfs mode error ", e);
        }

        return res;
    }

    public static String parseCifsAccess(String[] args) {
        StringBuilder res = new StringBuilder();
        Map<Long, String> accessToName = new HashMap<>();
        accessToName.put(SYNCHRONIZE, "SYNCHRONIZE");
        accessToName.put(WRITE_OWNER, "WRITE_OWNER");
        accessToName.put(WRITE_DACL, "WRITE_DACL");
        accessToName.put(READ_CONTROL, "READ_CONTROL");
        accessToName.put(DELETE, "DELETE");
        accessToName.put(WRITE_ATTR, "WRITE_ATTR");
        accessToName.put(READ_ATTR, "READ_ATTR");
        accessToName.put(DELETE_SUB_FILE_AND_DIR, "DELETE_SUB_FILE_AND_DIR");
        accessToName.put(LIST_DIR_OR_EXEC_FILE, "LIST_DIR_OR_EXEC_FILE");
        accessToName.put(WRITE_X_ATTR, "WRITE_X_ATTR");
        accessToName.put(READ_X_ATTR, "READ_X_ATTR");
        accessToName.put(MKDIR_OR_APPEND_DATA, "MKDIR_OR_APPEND_DATA");
        accessToName.put(CREATE_OR_WRITE_DATA, "CREATE_OR_WRITE_DATA");
        accessToName.put(LOOKUP_DIR_OR_READ_DATA, "LOOKUP_DIR_OR_READ_DATA");
        accessToName.put(MAXIMUM_ALLOWED, "MAXIMUM_ALLOWED");
        accessToName.put(ACCESS_SYSTEM_SECURITY, "ACCESS_SYSTEM_SECURITY");

        try {
            if (args.length < 2) {
                return res.toString();
            }

            String access = "";
            int num = 10;
            if (args.length == 2) {
                access = args[1];
            }

            if (args.length == 3) {
                access = args[1];
                num = Integer.parseInt(args[2]);
            }

            long right = Long.parseLong(access, num);
            for (long accFalg : accessToName.keySet()) {
                if ((right & accFalg) != 0) {
                    res.append(accessToName.get(accFalg) + "\n");
                }
            }

            log.info("parse access: {}, res: {}", access, res.toString());
        } catch (Exception e) {
            log.error("parse cifs access error", e);
        }

        return res.toString();
    }

    public static String parseCifsInherit(String[] args) {
        StringBuilder res = new StringBuilder();
        Map<Short, String> flagToName = new HashMap<>();
        flagToName.put(CONTAINER_INHERIT_ACE, "CONTAINER_INHERIT_ACE");
        flagToName.put(FAILED_ACCESS_ACE_FLAG, "FAILED_ACCESS_ACE_FLAG");
        flagToName.put(INHERIT_ONLY_ACE, "INHERIT_ONLY_ACE");
        flagToName.put(INHERITED_ACE, "INHERITED_ACE");
        flagToName.put(NO_PROPAGATE_INHERIT_ACE, "NO_PROPAGATE_INHERIT_ACE");
        flagToName.put(OBJECT_INHERIT_ACE, "OBJECT_INHERIT_ACE");
        flagToName.put(SUCCESSFUL_ACCESS_ACE_FLAG, "SUCCESSFUL_ACCESS_ACE_FLAG");

        try {
            if (args.length < 2) {
                return res.toString();
            }

            String flag = "";
            int num = 10;
            if (args.length == 2) {
                flag = args[1];
            }

            if (args.length == 3) {
                flag = args[1];
                num = Integer.parseInt(args[2]);
            }

            short right = Short.parseShort(flag, num);
            for (long inhFalg : flagToName.keySet()) {
                if ((right & inhFalg) != 0) {
                    res.append(flagToName.get(inhFalg) + "\n");
                }
            }

            log.info("parse flag: {}, res: {}", flag, res.toString());
        } catch (Exception e) {
            log.error("parse cifs flag error", e);
        }

        return res.toString();
    }

    public static String usefulCifsACL() {
        String PREFIX = "0x";
        String R = "R:" + PREFIX + Long.toHexString(1179785);
        String READ = "READ:" + PREFIX + Long.toHexString(1179817);
        String W = "W:" + PREFIX + Long.toHexString(1179926);
        String D = "D:" + PREFIX + Long.toHexString(65536);
        String P = "P:" + PREFIX + Long.toHexString(262144);
        String O = "O:" + PREFIX + Long.toHexString(524288);
        String X = "X:" + PREFIX + Long.toHexString(1179808);
        String CHANGE = "CHANGE:" + PREFIX + Long.toHexString(1245631);
        String FULL = "FULL:" + PREFIX + Long.toHexString(2032127);

        return R + "\n" +
                READ + "\n" +
                W + "\n" +
                D + "\n" +
                P + "\n" +
                O + "\n" +
                X + "\n" +
                CHANGE + "\n" +
                FULL;
    }

    public static String calCifsRight(String[] args) {
        StringBuilder res = new StringBuilder();
        Map<String, Long> nameToAccess = new HashMap<>();
        nameToAccess.put("SYNCHRONIZE", SYNCHRONIZE);
        nameToAccess.put("WRITE_OWNER", WRITE_OWNER);
        nameToAccess.put("WRITE_DACL", WRITE_DACL);
        nameToAccess.put("READ_CONTROL", READ_CONTROL);
        nameToAccess.put("DELETE", DELETE);
        nameToAccess.put("WRITE_ATTR", WRITE_ATTR);
        nameToAccess.put("READ_ATTR", READ_ATTR);
        nameToAccess.put("DELETE_SUB_FILE_AND_DIR", DELETE_SUB_FILE_AND_DIR);
        nameToAccess.put("LIST_DIR_OR_EXEC_FILE", LIST_DIR_OR_EXEC_FILE);
        nameToAccess.put("WRITE_X_ATTR", WRITE_X_ATTR);
        nameToAccess.put("READ_X_ATTR", READ_X_ATTR);
        nameToAccess.put("MKDIR_OR_APPEND_DATA", MKDIR_OR_APPEND_DATA);
        nameToAccess.put("CREATE_OR_WRITE_DATA", CREATE_OR_WRITE_DATA);
        nameToAccess.put("LOOKUP_DIR_OR_READ_DATA", LOOKUP_DIR_OR_READ_DATA);
        nameToAccess.put("MAXIMUM_ALLOWED", MAXIMUM_ALLOWED);
        nameToAccess.put("ACCESS_SYSTEM_SECURITY", ACCESS_SYSTEM_SECURITY);

        try {
            long right = 0;
            if (args.length <= 1) {
                return String.valueOf(right);
            }

            for (int i = 1; i < args.length; i++) {
                if (nameToAccess.containsKey(args[i])) {
                    right |= nameToAccess.get(args[i]);
                } else {
                    log.error("not contain: {}", args[i]);
                    right = -1;
                    break;
                }
            }

            log.info("parse access: {}, res: {}", right, res.toString());
        } catch (Exception e) {
            log.error("parse cifs access error", e);
        }

        return res.toString();
    }

    public static CliResponse printInfo(String[] args) {
        CliResponse response = new CliResponse();
        if (args.length != 2) {
            response.setStatus("please input correct parameters, printInfo all or printInfo xxxxxxxx");
        } else {
            try {
                if ("all".equals(args[1])) {
                    String printString =
                            "【uid】:" + ACLUtils.uidToS3ID + "\n" +
                                    "【gid】:" + ACLUtils.gidToS3ID + "\n" +
                                    "【userInfo】:" + ACLUtils.userInfo + "\n" +
                                    "【gids】:" + ACLUtils.s3IDToGids;

                    response.setStatus(printString);
                    log.info("【printInfo】:{}", printString);
                } else if (12 == args[1].length()) { //12是s3Id的长度
                    String s3Id = args[1];
                    long id = Long.parseLong(s3Id);
                    String userInfo = ACLUtils.userInfo.get(s3Id) + "";
                    String gids = ACLUtils.s3IDToGids.get(s3Id) + "";
                    String printString =
                            "【userInfo】:" + userInfo + "\n" +
                                    "【gids】:" + gids;
                    response.setStatus(printString);
                } else if (args[1].startsWith("uid:")) {
                    String[] split = args[1].split(":");
                    int uid = Integer.parseInt(split[1]);
                    String printString =
                            "【uid】:" + uid + "\n" +
                                    "【S3Id】:" + ACLUtils.uidToS3ID.get(uid);
                    response.setStatus(printString);
                } else if (args[1].startsWith("gid:")) {
                    String[] split = args[1].split(":");
                    int gid = Integer.parseInt(split[1]);
                    String printString =
                            "【gid】:" + gid + "\n" +
                                    "【S3Id】:" + ACLUtils.gidToS3ID.get(gid);
                    response.setStatus(printString);
                } else {
                    response.setStatus("please input correct parameters, printInfo all or printInfo xxxxxxxx");
                }
            } catch (Exception e) {
                log.error("【printInfo】 error", e);
                response.setStatus("please input correct parameters, printInfo all or printInfo xxxxxxxx");
            }
        }

        return response;
    }

    public static int updateFsPort() {
        try {
            int nfsPort = FsUtils.getFsPort(FsConstants.FSConfig.NFS_PORT, NFS.nfsPort);
            int mountPort = FsUtils.getFsPort(FsConstants.FSConfig.NFS_MOUNT_PORT, NFS.mountPort);
            int nlmPort = FsUtils.getFsPort(FsConstants.FSConfig.NLM_PORT, NFS.nlmPort);
            int nsmPort = FsUtils.getFsPort(FsConstants.FSConfig.NSM_PORT, NFS.nsmPort);
            int nfsRQuotaPort = FsUtils.getFsPort(FsConstants.FSConfig.NFS_QUOTA_PORT, NFS.nfsRQuotaPort);

            boolean isRestartNfs = false;
            if (nfsPort != NFS.nfsPort) {
                NFS.nfsPort = nfsPort;
                isRestartNfs = true;
            }

            if (mountPort != NFS.mountPort) {
                NFS.mountPort = mountPort;
                isRestartNfs = true;
            }

            if (nlmPort != NFS.nlmPort) {
                NFS.nlmPort = nlmPort;
                isRestartNfs = true;
            }

            if (nsmPort != NFS.nsmPort) {
                NFS.nsmPort = nsmPort;
                isRestartNfs = true;
            }

            if (nfsRQuotaPort != NFS.nfsRQuotaPort) {
                NFS.nfsRQuotaPort = nfsRQuotaPort;
                isRestartNfs = true;
            }

            if (isRestartNfs) {
                NFS.restart();
            }


            int cifsPort = FsUtils.getFsPort(FsConstants.FSConfig.CIFS_PORT, CIFS.cifsPort);
            if (cifsPort != CIFS.cifsPort) {
                CIFS.cifsPort = cifsPort;
                CIFS.restart();
            }

            int ftpControllerPort = FsUtils.getFsPort(FsConstants.FSConfig.FTP_CONTROL_PORT, FTP.ftpControllerPort);
            int ftpsControllerPort = FsUtils.getFsPort(FsConstants.FSConfig.FTPS_CONTROL_PORT, FTP.ftpsControllerPort);

            boolean isRestartFtp = false;
            if (ftpControllerPort != FTP.ftpControllerPort) {
                FTP.ftpControllerPort = ftpControllerPort;
                isRestartFtp = true;
            }

            if (ftpsControllerPort != FTP.ftpsControllerPort) {
                FTP.ftpsControllerPort = ftpsControllerPort;
                isRestartFtp = true;
            }

            if (isRestartFtp) {
                FTP.restart();
            }

            log.info("update fs port success");
        } catch (Exception e) {
            log.error("update fs port error, ", e);
            return -1;
        }

        return 1;
    }
}

package com.macrosan.action.datastream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.ec.ErasureClient;
import com.macrosan.ec.Utils;
import com.macrosan.ec.server.ErasureServer;
import com.macrosan.filesystem.FsConstants;
import com.macrosan.filesystem.utils.CheckUtils;
import com.macrosan.filesystem.utils.InodeUtils;
import com.macrosan.filesystem.utils.acl.ACLUtils;
import com.macrosan.filesystem.utils.acl.CIFSACL;
import com.macrosan.filesystem.utils.acl.NFSACL;
import com.macrosan.httpserver.MsHttpRequest;
import com.macrosan.message.jsonmsg.Inode;
import com.macrosan.message.jsonmsg.MetaData;
import com.macrosan.message.xmlmsg.AccessControlPolicy;
import com.macrosan.message.xmlmsg.FsACE;
import com.macrosan.message.xmlmsg.FsACL;
import com.macrosan.message.xmlmsg.section.Grant;
import com.macrosan.message.xmlmsg.section.Grantee;
import com.macrosan.message.xmlmsg.section.Owner;
import com.macrosan.storage.StoragePool;
import com.macrosan.storage.StoragePoolFactory;
import com.macrosan.utils.authorize.AuthorizeV4;
import com.macrosan.utils.functional.Entry;
import com.macrosan.utils.functional.Tuple3;
import com.macrosan.utils.msutils.MsAclUtils;
import com.macrosan.utils.msutils.MsException;
import com.macrosan.utils.policy.ReactorPolicyCheckUtils;
import com.macrosan.utils.serialize.JaxbUtils;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.list.mutable.FastList;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.util.function.Tuple2;

import java.util.*;

import static com.macrosan.action.datastream.ActiveService.PASSWORD;
import static com.macrosan.action.datastream.ActiveService.SYNC_AUTH;
import static com.macrosan.constants.ErrorNo.*;
import static com.macrosan.constants.ServerConstants.*;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.filesystem.FsConstants.ACLConstants.PROTO;
import static com.macrosan.httpserver.ResponseUtils.addAllowHeader;
import static com.macrosan.httpserver.ResponseUtils.addPublicHeaders;
import static com.macrosan.message.consturct.RequestBuilder.getRequestId;
import static com.macrosan.utils.msutils.MsException.dealException;
import static com.macrosan.utils.msutils.MsException.throwWhenEmpty;

/**
 * 处理acl相关接口的请求
 *
 * @auther wuhaizhong
 * @date 2020/5/6
 */
public class ObjectAclService extends BaseService {

    private static final Logger logger = LogManager.getLogger(ObjectAclService.class.getName());

    private static ObjectAclService instance = null;

    private static final Map<String, Integer> objGrantPermissionMap = new HashMap<>(4);

    private ObjectAclService() {
        super();
    }

    public static ObjectAclService getInstance() {
        if (instance == null) {
            instance = new ObjectAclService();
        }
        objGrantPermissionMap.put(PERMISSION_READ, OBJECT_PERMISSION_READ_NUM);
        objGrantPermissionMap.put(PERMISSION_READ_CAP, OBJECT_PERMISSION_READ_CAP_NUM);
        objGrantPermissionMap.put(PERMISSION_WRITE_CAP, OBJECT_PERMISSION_WRITE_CAP_NUM);
        objGrantPermissionMap.put(PERMISSION_FULL_CON, OBJECT_PERMISSION_FULL_CON_NUM);
        return instance;
    }

    /**
     * 设置指定版本对象权限,只有对象的拥有者或者同时具有READ_ACP权限和WRITE_ACP权限的账户有权操作
     *
     * @param request 请求
     * @return 响应的返回值
     */
    public int putObjectVersionAcl(MsHttpRequest request) {
        return putObjectAcl(request);
    }

    /**
     * 设置对象权限,只有对象的拥有者或者同时具有READ_ACP权限和WRITE_ACP权限的账户有权操作
     *
     * @param request 请求
     * @return 响应的返回值
     */
    public int putObjectAcl(MsHttpRequest request) {
        String userId = request.getUserId();
        String bucketName = request.getBucketName();
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        String objName = request.getObjectName();
        com.macrosan.utils.functional.Tuple2<String, String> bucketVnodeIdTuple = storagePool.getBucketVnodeIdTuple(bucketName, objName);
        String bucketVnode = bucketVnodeIdTuple.var1;
        String migrateVnode = bucketVnodeIdTuple.var2;
        final String[] bucketUserId = new String[1];
        final String[] versionId = new String[1];
        final int[] policy = new int[1];
        String contentLength = request.getHeader(CONTENT_LENGTH);
        final String method = StringUtils.isBlank(request.getHeader(VERSIONID)) ? "PutObjectAcl" : "PutObjectVersionAcl";

        FastList<Entry<String, String>> grantAclList = new FastList<>(16);
        request.headers().entries().forEach(entry -> {
            if (entry.getKey().startsWith("x-amz-grant-")) {
                for (String value : entry.getValue().split(",")) {
                    String[] id = value.split("=", 2);
                    if (id.length != 2) {
                        throw new MsException(ErrorNo.INVALID_ARGUMENT, "acl input error.");
                    }
                    grantAclList.add(new Entry<>(id[1], entry.getKey()));
                }
            }
        });

        if (request.headers().contains("x-amz-acl") && grantAclList.size() > 0) {
            throw new MsException(INVALID_ARGUMENT, "the acl input is invlid");
        }

        final String[] currentSnapshotMark = new String[]{null};
        final String[] snapshotLink = new String[]{null};
        boolean[] isInnerObject = new boolean[]{request.headers().contains(CLUSTER_ALIVE_HEADER)};
        boolean[] hasStartFS = {false};
        Disposable subscribe = pool.getReactive(REDIS_BUCKETINFO_INDEX)
                .hgetall(bucketName)
                .doOnNext(bucketInfo -> {
                    if (bucketInfo.isEmpty()) {
                        throw new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket name :" + bucketName + ".");
                    }
                    regionCheck(bucketInfo.get(REGION_NAME));
                    siteCheck(bucketInfo);
                    CheckUtils.hasStartFS(bucketInfo, hasStartFS);
                    bucketUserId[0] = bucketInfo.get(BUCKET_USER_ID);
                    versionId[0] = StringUtils.isNotEmpty(request.getParam(VERSIONID)) ? request.getParam(VERSIONID)
                            : bucketInfo.containsKey(BUCKET_VERSION_STATUS) ? "" : "null";
                    versionId[0] = StringUtils.isBlank(request.getHeader(VERSIONID)) ? versionId[0] : request.getHeader(VERSIONID);
                    currentSnapshotMark[0] = bucketInfo.get(CURRENT_SNAPSHOT_MARK);
                    snapshotLink[0] = bucketInfo.get(SNAPSHOT_LINK);
                })
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyObjectAclCheckResult(request, bucketName, objName, versionId[0], method, grantAclList))
                .doOnNext(access -> policy[0] = access)
                .flatMap(access -> storagePool.mapToNodeInfo(bucketVnode))
                .flatMap(list -> ErasureClient.getObjectMetaVersionResUnlimited(bucketName, objName, versionId[0], list, request, currentSnapshotMark[0], snapshotLink[0]).zipWith(Mono.just(list)))
                .doOnNext(tuple2 -> checkMetaData(objName, tuple2.getT1().var1, request.getParam(VERSIONID), currentSnapshotMark[0]))
                .flatMap(tuple2 -> {
                    MetaData metaData = tuple2.getT1().var1;
                    logger.debug("metaData:{}", metaData);
                    String syncStamp = request.getHeader(SYNC_STAMP);
                    if (StringUtils.isNotBlank(syncStamp) && !syncStamp.equals(metaData.syncStamp)) {
                        logger.info("syncStamp is different,curr is {} sync is {}", metaData, syncStamp);
                        throw new MsException(ErrorNo.SYNC_DATA_MODIFIED, "");
                    }

                    Map<String, String> sysMap = Json.decodeValue(metaData.getSysMetaData(), new TypeReference<Map<String, String>>() {
                    });

                    isInnerObject[0] = isInnerObject[0]
                            && sysMap.containsKey(NO_SYNCHRONIZATION_KEY)
                            && sysMap.get(NO_SYNCHRONIZATION_KEY).equals(NO_SYNCHRONIZATION_VALUE);

                    Map<String, String> objectAclMap = Json.decodeValue(metaData.getObjectAcl(), new TypeReference<Map<String, String>>() {
                    });
                    logger.debug("objectAclMap:" + objectAclMap);
                    String objectUserId = objectAclMap.get("owner");
                    if (!PASSWORD.equals(request.getHeader(SYNC_AUTH)) && policy[0] == 0) {
                        if (hasStartFS[0]) {
                            MsAclUtils.checkFsObjAcp(objectAclMap, userId, bucketUserId[0], OBJECT_PERMISSION_WRITE_CAP_NUM, metaData);
                        } else {
                            MsAclUtils.checkObjAcp(objectAclMap, userId, bucketUserId[0], OBJECT_PERMISSION_WRITE_CAP_NUM);
                        }
                    }
                    if (request.headers().contains(ACL_HEADER)) {
                        metaData.setObjectAcl(request.headers().get(ACL_HEADER));
                        return setObjectAcl(metaData, bucketVnode, migrateVnode, tuple2.getT2(), bucketUserId[0], request, tuple2.getT1().var2, currentSnapshotMark[0], snapshotLink[0]);
                    }
                    if (0 == Integer.parseInt(contentLength)) {
                        if (grantAclList.size() > 0) {
                            MonoProcessor<Boolean> res = MonoProcessor.create();
                            setObjectGrantAcl(res, metaData, objectUserId, bucketUserId[0], bucketVnode, migrateVnode, grantAclList, tuple2.getT2(), request, tuple2.getT1().var2, currentSnapshotMark[0], snapshotLink[0]);
                            return res;
                        } else if (request.headers().contains("x-amz-acl")) {
                            String objectAcl = request.getHeader("x-amz-acl");
                            objectAcl = MsAclUtils.objAclCheck(objectAcl);
                            objectAclMap.clear();
                            objectAclMap.put("owner", objectUserId);
                            objectAclMap.put("acl", objectAcl);
                            metaData.setObjectAcl(Json.encode(objectAclMap));
                            return setObjectAcl(metaData, bucketVnode, migrateVnode, tuple2.getT2(), bucketUserId[0], request, tuple2.getT1().var2, currentSnapshotMark[0], snapshotLink[0]);

                        } else {
                            throw new MsException(MISSING_SECURITY_HEADER, "missing required acl header");
                        }

                    } else {
                        if (request.headers().contains("x-amz-acl") || grantAclList.size() > 0) {
                            throw new MsException(UNEXPECTEDCONTENT, "set acl does not support both header and content");
                        }
                        responseContinue(request);
                        Buffer buffer = Buffer.buffer();
                        MonoProcessor<Boolean> res = MonoProcessor.create();
                        request.handler(buffer::appendBuffer)
                                .endHandler(v -> {
                                    try {
                                        int code = AuthorizeV4.checkManageStreamPayloadSHA256(request, buffer.getBytes());
                                        if (code != ErrorNo.SUCCESS_STATUS) {
                                            return;
                                        }
                                        AccessControlPolicy result = (AccessControlPolicy) JaxbUtils.toObject(new String(buffer.getBytes()));
                                        List<Grant> grantList = result.getAccessControlList();
                                        FastList<Entry<String, String>> xmlList = new FastList<>(grantList.size());
                                        for (Grant grant : grantList) {
                                            Entry<String, String> xmlEntry = new Entry<>(null, null);
                                            xmlEntry.setKey(grant.getGrantee().getId());
                                            xmlEntry.setValue(grant.getPermission());
                                            xmlList.add(xmlEntry);
                                        }
                                        setObjectGrantAcl(res, metaData, objectUserId, bucketUserId[0], bucketVnode, migrateVnode, xmlList, tuple2.getT2(), request, tuple2.getT1().var2, currentSnapshotMark[0], snapshotLink[0]);
                                    } catch (Exception e) {
                                        logger.error(e);
                                        MsException.dealException(request, e);
                                    }
                                })
                                .exceptionHandler(logger::error)
                                .resume();
                        return res;
                    }
                })
                .subscribe(b -> {
                    if (!b) {
                        dealException(request, new MsException(UNKNOWN_ERROR, "set object acl error!"));
                    } else {
                        //response返回
                        HttpServerResponse httpServerResponse = addPublicHeaders(request, getRequestId())
                                .putHeader(CONTENT_LENGTH, "0");

                        if (isInnerObject[0]) {
                            httpServerResponse.putHeader(IFF_TAG, "1");
                        }

                        httpServerResponse
                                .setStatusCode(200);
                        addAllowHeader(request.response()).end();
                    }
                }, e -> {
                    if (e instanceof MsException && ErrorNo.SYNC_DATA_MODIFIED == ((MsException) e).getErrCode()) {
                        //数据同步已修改，返回成功
                        request.response()
                                .putHeader(CONTENT_LENGTH, "0")
                                .setStatusCode(200)
                                .end();
                    } else {
                        dealException(request, e);
                    }
                });

        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));

        return ErrorNo.SUCCESS_STATUS;
    }

    /**
     * 获取指定版本的object权限,只有对象的拥有者或者具有READ_ACP权限
     *
     * @param request 请求
     * @return ResponseMsg
     */
    public int getObjectVersionAcl(MsHttpRequest request) {
        return getObjectAcl(request);
    }

    /**
     * 获取object权限,只有对象的拥有者或者具有READ_ACP权限
     *
     * @param request 请求
     * @return ResponseMsg
     */
    public int getObjectAcl(MsHttpRequest request) {
        String userId = request.getUserId();
        String bucketName = request.getBucketName();
        String objName = request.getObjectName();
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        String bucketVnode = storagePool.getBucketVnodeId(bucketName, objName);
        final int[] policy = new int[1];
        final String method = StringUtils.isBlank(request.getHeader(VERSIONID)) ? "GetObjectAcl" : "GetObjectVersionAcl";
        final String[] versionId = new String[1];
        final String[] currentSnapshotMark = new String[]{null};
        final String[] snapshotLink = new String[]{null};
        Disposable subscribe = pool.getReactive(REDIS_BUCKETINFO_INDEX)
                .hgetall(bucketName)
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo,
                        new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket name :" + bucketName + ".")))
                .doOnNext(bucketInfo -> regionCheck(bucketInfo.get(REGION_NAME)))
                .doOnNext(this::siteCheck)
                .doOnNext(bucketInfo -> versionId[0] = StringUtils.isNotEmpty(request.getParam(VERSIONID)) ? request.getParam(VERSIONID)
                        : bucketInfo.containsKey(BUCKET_VERSION_STATUS) ? "" : "null")
                .doOnNext(bucketInfo -> {
                    currentSnapshotMark[0] = bucketInfo.get(CURRENT_SNAPSHOT_MARK);
                    snapshotLink[0] = bucketInfo.get(SNAPSHOT_LINK);
                })
                .flatMap(bucketInfo -> ReactorPolicyCheckUtils.getPolicyGetCheckResult(request, bucketName, objName, versionId[0], method).zipWith(Mono.just(bucketInfo)))
                .doOnNext(tuple2 -> policy[0] = tuple2.getT1())
                .map(Tuple2::getT2)
                .flatMap(bucketInfo -> storagePool.mapToNodeInfo(bucketVnode).zipWith(Mono.just(bucketInfo)))
                .flatMap(tuple2 -> ErasureClient.getObjectMetaVersion(bucketName, objName, versionId[0], tuple2.getT1(), request, currentSnapshotMark[0], snapshotLink[0])
                        .doOnNext(metaData -> checkMetaData(objName, metaData, request.getParam(VERSIONID), currentSnapshotMark[0]))
                        .zipWith(Mono.just(tuple2.getT2())))
                .flatMap(tuple2 -> {
                    MetaData metaData = tuple2.getT1();
                    Map<String, String> objAclMap = Json.decodeValue(metaData.getObjectAcl(), new TypeReference<Map<String, String>>() {
                    });
                    String objectUserId = objAclMap.get("owner");
                    return pool.getReactive(REDIS_USERINFO_INDEX).hget(objectUserId, USER_DATABASE_ID_NAME).zipWith(Mono.just(tuple2));
                })
                .flatMap(tuple2 -> {
                    Tuple2<MetaData, Map<String, String>> t1 = tuple2.getT2();
                    String objectUserName = tuple2.getT1();
                    MetaData metaData = t1.getT1();
                    Map<String, String> bucketInfo = t1.getT2();
                    String bucketUserId = bucketInfo.get(BUCKET_USER_ID);
                    String bucketUser = bucketInfo.get(BUCKET_USER_NAME);
                    Map<String, String> objAclMap = Json.decodeValue(metaData.getObjectAcl(), new TypeReference<Map<String, String>>() {
                    });
                    String objectUserId = objAclMap.get("owner");
                    int objectAclNum = Integer.parseInt(objAclMap.get("acl"));
                    if (!PASSWORD.equals(request.getHeader(SYNC_AUTH)) && policy[0] == 0) {
                        if (bucketInfo.containsKey("fsid")) {
                            MsAclUtils.checkFsObjAcp(objAclMap, userId, bucketUserId, OBJECT_PERMISSION_READ_CAP_NUM, metaData);
                        } else {
                            MsAclUtils.checkObjAcp(objAclMap, userId, bucketUserId, OBJECT_PERMISSION_READ_CAP_NUM);
                        }

                    }
                    Grantee ownerGrantee = new Grantee()
                            .setId(objectUserId)
                            .setDisplayName(objectUserName);
                    Grant ownerGrant = new Grant()
                            .setGrantee(ownerGrantee)
                            .setPermission(PERMISSION_FULL_CON);
                    List<Grant> list = new FastList<Grant>(16).with(ownerGrant);
                    getCanonicalAcl(bucketUserId, bucketUser, objectUserId, objectAclNum, list);
                    return getObjectGrantAcl(objAclMap, objectAclNum, list)
                            .map(grantList -> {
                                logger.debug("list:" + list);
                                Owner owner = new Owner()
                                        .setId(objectUserId)
                                        .setDisplayName(objectUserName);
                                AccessControlPolicy accessControlPolicy = new AccessControlPolicy()
                                        .setOwner(owner)
                                        .setAccessControlList(grantList);
                                logger.debug("Get object acl successful, object:" + objName);
                                return JaxbUtils.toByteArray(accessControlPolicy);
                            });
                })
                .subscribe(data -> {
                    //response返回
                    addPublicHeaders(request, getRequestId())
                            .putHeader(CONTENT_TYPE, "application/xml")
                            .putHeader(CONTENT_LENGTH, String.valueOf(data.length))
                            .write(Buffer.buffer(data));
                    addAllowHeader(request.response()).end();
                }, e -> dealException(request, e));
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
        return ErrorNo.SUCCESS_STATUS;
    }


    /**
     * 设置对象私有权限
     *
     * @param metaData     原元数据
     * @param objectUserId 对象所有者
     * @param bucketUserId 桶所有者
     * @param bucketVnode  桶对应vnode
     * @param xmlList      acl List
     * @param request
     */
    public void setObjectGrantAcl(MonoProcessor<Boolean> res, MetaData metaData, String objectUserId, String bucketUserId,
                                  String bucketVnode, String migrateVnode, List<Entry<String, String>> xmlList,
                                  List<Tuple3<String, String, String>> nodeList, MsHttpRequest request,
                                  com.macrosan.utils.functional.Tuple2<ErasureServer.PayloadMetaType, MetaData>[] metaRes, String currentSnapshotMark, String snapshotLink) {
        if (xmlList.isEmpty()) {
            throw new MsException(ErrorNo.NO_SUCH_OBJECT_PERMISSION, "setObjectGrantAcl error, not acl info when merge.");
        }
        Disposable subscribe = Flux.fromIterable(xmlList)
                .filter(entry -> !entry.getValue().equals("DEFAULT"))
                .flatMap(this::checkTypeAndUserId)
                .collectList()
                .map(list -> {
                    int objectAclNum = OBJECT_PERMISSION_PRIVATE_NUM;
                    for (Entry<String, String> map : xmlList) {
                        String aclType = map.getValue();
                        String targetUserId = map.getKey();
                        if (StringUtils.isBlank(targetUserId)) {
                            if (PERMISSION_READ.equals(aclType)) {
                                objectAclNum = OBJECT_PERMISSION_SHARE_READ_NUM;
                            }
                            if (PERMISSION_WRITE.equals(aclType)) {
                                objectAclNum = OBJECT_PERMISSION_SHARE_READ_WRITE_NUM;
                            }
                        }
                    }
                    return objectAclNum;
                })
                .map(objectAclNum -> {
                    Map<String, String> aclMap = new HashMap<>();
                    objectAclNum = (objectAclNum & OBJECT_PERMISSION_CLEAR_GRANT_NUM);
                    aclMap.put("acl", String.valueOf(objectAclNum));
                    aclMap.put("owner", objectUserId);
                    for (Entry<String, String> map : xmlList) {
                        String aclId = map.getKey();
                        String aclType = map.getValue();
                        if (aclId == null || aclId.isEmpty()) {
                            continue;
                        }
                        if (aclType.equalsIgnoreCase(PERMISSION_READ) || aclType.equalsIgnoreCase(PERMISSION_READ_LONG)) {
                            aclMap = addObjectIfAbsent(aclMap, objectAclNum, Integer.parseInt(aclMap.get("acl")), objectUserId, aclId, bucketUserId, OBJECT_PERMISSION_READ_NUM);
                        } else if (aclType.equalsIgnoreCase(PERMISSION_READ_CAP) || aclType.equalsIgnoreCase(PERMISSION_READ_CAP_LONG)) {
                            aclMap = addObjectIfAbsent(aclMap, objectAclNum, Integer.parseInt(aclMap.get("acl")), objectUserId, aclId, bucketUserId, OBJECT_PERMISSION_READ_CAP_NUM);
                        } else if (aclType.equalsIgnoreCase(PERMISSION_WRITE_CAP) || aclType.equalsIgnoreCase(PERMISSION_WRITE_CAP_LONG)) {
                            aclMap = addObjectIfAbsent(aclMap, objectAclNum, Integer.parseInt(aclMap.get("acl")), objectUserId, aclId, bucketUserId, OBJECT_PERMISSION_WRITE_CAP_NUM);
                        } else {
                            aclMap = addObjectIfAbsent(aclMap, objectAclNum, Integer.parseInt(aclMap.get("acl")), objectUserId, aclId, bucketUserId, OBJECT_PERMISSION_FULL_CON_NUM);
                        }
                    }
                    logger.debug("aclMap:" + aclMap);
                    //修改对象acl
                    return metaData.setObjectAcl(Json.encode(aclMap));
                })
                .flatMap(meta -> setObjectAcl(meta, bucketVnode, migrateVnode, nodeList, bucketUserId, request, metaRes, currentSnapshotMark, snapshotLink))
                .subscribe(res::onNext, res::onError);
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
    }

    private Mono<Entry<String, String>> checkTypeAndUserId(Entry<String, String> grantArray) {
        String targetUserId = grantArray.getKey();
        String acltype = grantArray.getValue();
        return Mono.justOrEmpty(grantArray.getKey())
                .doOnNext(key -> checkObjPermissionType(acltype))
                .filter(StringUtils::isNotBlank)
                .flatMap(userId -> pool.getReactive(REDIS_USERINFO_INDEX)
                        .hget(targetUserId, USER_DATABASE_HASH_TYPE)
                        .defaultIfEmpty("none"))
                .map(type -> {
                    if (!USER_DATABASE_ID_TYPE.equals(type)) {
                        logger.error("no such user id. user id: ");
                        throw new MsException(ErrorNo.NO_SUCH_ID, "no such user id. user id: " + targetUserId + ".");
                    }
                    return grantArray;
                });
    }

    private void checkObjPermissionType(String aclType) {
        if (!PERMISSION_READ.equals(aclType)
                && !PERMISSION_READ_LONG.equals(aclType)
                && !PERMISSION_WRITE.equals(aclType)
                && !PERMISSION_READ_CAP.equals(aclType)
                && !PERMISSION_READ_CAP_LONG.equals(aclType)
                && !PERMISSION_WRITE_CAP.equals(aclType)
                && !PERMISSION_WRITE_CAP_LONG.equals(aclType)
                && !PERMISSION_FULL_CON.equals(aclType)
                && !PERMISSION_FULL_CON_LONG.equals(aclType)) {
            throw new MsException(ErrorNo.NO_SUCH_OBJECT_PERMISSION, "No such object permission.");
        }
    }

    public Map<String, String> addObjectIfAbsent(Map<String, String> aclMap, int objNum, int aclNum, String objectUserId, String aclId, String bucketUserId, int permissionNum) {
        String perNum = String.valueOf(permissionNum);
        if (objectUserId.equals(aclId)) {
            return aclMap;
        }
        if ((objNum == OBJECT_PERMISSION_SHARE_OWNER_FULL_CONTROL_NUM && bucketUserId.equals(aclId)) ||
                (objNum == OBJECT_PERMISSION_SHARE_BUCKET_OWNER_READ_NUM && permissionNum == OBJECT_PERMISSION_READ_NUM && bucketUserId.equals(aclId))) {
            return aclMap;
        }
        if (aclMap.containsKey(perNum + "-" + aclId)) {
            return aclMap;
        } else {
            aclNum |= permissionNum;
            aclMap.put(perNum + "-" + aclId, perNum);
            aclMap.replace("acl", aclMap.get("acl"), String.valueOf(aclNum));
        }
        return aclMap;
    }

    /**
     * EC组各节点rocksdb设置元数据
     *
     * @param metaData    元数据
     * @param bucketVnode 桶对应vnode
     * @param request
     */
    public Mono<Boolean> setObjectAcl(MetaData metaData, String bucketVnode, String migrateVnode, List<Tuple3<String, String, String>> nodeList,
                                      String bucketUserId, MsHttpRequest request, com.macrosan.utils.functional.Tuple2<ErasureServer.PayloadMetaType, MetaData>[] metaRes, String currentSnapshotMark, String snapshotLink) {
        String userId = request.getUserId();
        String bucketName = request.getBucketName();
        String objectName = request.getObjectName();
        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);

        return Mono.just(StringUtils.isNotEmpty(migrateVnode))
                .flatMap(b -> {
                    if (b) {
                        return storagePool.mapToNodeInfo(migrateVnode)
                                .flatMap(migrateVnodeList -> ErasureClient.getObjectMetaVersionResOnlyRead(bucketName, objectName, metaData.versionId, migrateVnodeList, request, currentSnapshotMark, snapshotLink).zipWith(Mono.just(migrateVnodeList)))
                                .flatMap(tuple2 -> ErasureClient.updateMetaDataAcl(Utils.getVersionMetaDataKey(migrateVnode, bucketName, objectName, metaData.versionId, metaData.snapshotMark),
                                        metaData.clone(), tuple2.getT2(), request, tuple2.getT1().var2))
                                .map(r -> r == 1);
                    }
                    return Mono.just(true);
                })
                .doOnNext(b -> {
                    if (!b) {
                        throw new MsException(UNKNOWN_ERROR, "update bucket "+ bucketName +" object " + objectName + " acl to new vnode error");
                    }
                })
                .flatMap(b -> storagePool.mapToNodeInfo(bucketVnode))
                .flatMap(bucketNodeList -> ErasureClient.updateMetaDataAcl(Utils.getVersionMetaDataKey(bucketVnode, bucketName, objectName, metaData.versionId, metaData.snapshotMark),
                        metaData, bucketNodeList, request, metaRes))
                .flatMap(b -> {
                    if (b != 2) {
                        return Mono.just(b == 1);
                    }
                    //元数据覆盖，重试一次！
                    return ErasureClient.getObjectMetaVersionResUnlimited(bucketName, objectName, metaData.versionId, nodeList, request, currentSnapshotMark, snapshotLink)
                            .doOnNext(tuple2 -> {
                                checkMetaData(objectName, tuple2.var1, request.getParam(VERSIONID), currentSnapshotMark);
                                Map<String, String> objectAclMap = Json.decodeValue(tuple2.var1.getObjectAcl(), new TypeReference<Map<String, String>>() {
                                });
                                if (!PASSWORD.equals(request.getHeader(SYNC_AUTH))) {
                                    MsAclUtils.checkObjAcp(objectAclMap, userId, bucketUserId, OBJECT_PERMISSION_WRITE_CAP_NUM);
                                }
                            })
                            .flatMap(tuple2 -> {
                                MetaData metaData1 = tuple2.var1;
                                metaData1.setObjectAcl(metaData.getObjectAcl());
                                return storagePool.mapToNodeInfo(bucketVnode).flatMap(bucketNodeList ->
                                        ErasureClient.updateMetaDataAcl(Utils.getVersionMetaDataKey(bucketVnode, bucketName, objectName, metaData.versionId, metaData.snapshotMark),
                                                metaData1, bucketNodeList, request, tuple2.var2));
                            })
                            .flatMap(s -> Mono.just(s == 1));
                });
    }


    private void getCanonicalAcl(String bucketUserId, String bucketUser, String objectUserId, int objectAclNum, List<Grant> list) {
        Grantee objectGrantee = new Grantee()
                .setId("")
                .setDisplayName("ALLUsers");
        if ((objectAclNum & OBJECT_PERMISSION_SHARE_BUCKET_OWNER_READ_NUM) != 0) {
            //判断对象拥有者与桶拥有者不同
            if (!bucketUserId.equals(objectUserId)) {
                Grantee grantee = new Grantee()
                        .setId(bucketUserId)
                        .setDisplayName(bucketUser);
                Grant grant = new Grant()
                        .setGrantee(grantee)
                        .setPermission(PERMISSION_READ);
                list.add(grant);
            }
        } else if ((objectAclNum & OBJECT_PERMISSION_SHARE_OWNER_FULL_CONTROL_NUM) != 0) {
            if (!bucketUserId.equals(objectUserId)) {
                Grantee grantee = new Grantee()
                        .setId(bucketUserId)
                        .setDisplayName(bucketUser);
                Grant grant = new Grant()
                        .setGrantee(grantee)
                        .setPermission(PERMISSION_FULL_CON);
                list.add(grant);
            }
        } else if ((objectAclNum & OBJECT_PERMISSION_SHARE_READ_NUM) != 0) {
            Grant bucketGrant = new Grant()
                    .setGrantee(objectGrantee)
                    .setPermission(PERMISSION_READ);
            list.add(bucketGrant);
        } else if ((objectAclNum & OBJECT_PERMISSION_SHARE_READ_WRITE_NUM) != 0) {
            Grant bucketGrant = new Grant()
                    .setGrantee(objectGrantee)
                    .setPermission(PERMISSION_READ);
            list.add(bucketGrant);

            bucketGrant = new Grant()
                    .setGrantee(objectGrantee)
                    .setPermission(PERMISSION_WRITE);
            list.add(bucketGrant);
        } else if ((objectAclNum & OBJECT_PERMISSION_PRIVATE_NUM) == 0) {
            throw new MsException(ErrorNo.UNKNOWN_ERROR, "Get object acl type error.");
        }
    }

    /**
     * @param objAclMap 对象元数据信息
     * @param aclNum    对象ACL数
     * @param list      xml列表
     */
    public Mono<List<Grant>> getObjectGrantAcl(Map<String, String> objAclMap, int aclNum, List<Grant> list) {
        return Flux.fromIterable(objGrantPermissionMap.entrySet())
                .filter(entry -> (aclNum & entry.getValue()) != 0)
                .flatMap(entry -> getObjectGrantAclByType(objAclMap, entry.getKey(), entry.getValue(), list))
                .collectList()
                .map(s -> list);
    }

    /**
     * 根据不同的object-grant类型获取object权限
     *
     * @param objAclMap 对象权限信息
     * @param aclType   acl类型
     * @param list      xml列表
     */
    private Mono<List<Grant>> getObjectGrantAclByType(Map<String, String> objAclMap, String aclType, int aclTypeNum, List<Grant> list) {
        objAclMap.remove("acl");
        objAclMap.remove("owner");

        logger.debug("objAclMap" + objAclMap);
        return Flux.fromIterable(objAclMap.entrySet())
                .filter(entry -> aclTypeNum == Integer.parseInt(entry.getKey().split("-")[0]))
                .flatMap(entry -> {
                    String id = entry.getKey().split("-")[1];
                    Grantee objectGrantee = new Grantee().setId(id);
                    return pool.getReactive(REDIS_USERINFO_INDEX)
                            .hget(id, USER_DATABASE_ID_NAME)
                            .map(objectGrantee::setDisplayName);
                })
                .map(objectGrantee -> new Grant().setGrantee(objectGrantee).setPermission(aclType))
                .doOnNext(grant -> {
                    if (!list.contains(grant)) {
                        list.add(grant);
                    }
                })
                .collectList()
                .switchIfEmpty(Mono.just(list))
                .map(l -> list);
    }

    /**
     * 获取文件或目录的权限信息
     *
     * @param request 请求
     * @return ResponseMsg
     */
    public int getFsAcl(MsHttpRequest request) {
        String userId = request.getUserId();
        String bucketName = request.getBucketName();
        String objName = request.getObjectName();
        String protoType = request.getParam(PROTO);
        boolean[] nfsV3 = {false};
        boolean[] cifs = {false};

        if (StringUtils.isBlank(protoType) || !isProtoSupport(protoType, nfsV3, cifs)) {
            throw new MsException(INVALID_ARGUMENT, "the protoType is invalid.");
        }

        StoragePool storagePool = StoragePoolFactory.getMetaStoragePool(bucketName);
        String bucketVnode = storagePool.getBucketVnodeId(bucketName, objName);

        final String[] versionId = new String[1];
        final String[] currentSnapshotMark = new String[]{null};
        final String[] snapshotLink = new String[]{null};

        Disposable subscribe = pool.getReactive(REDIS_BUCKETINFO_INDEX)
                .hgetall(bucketName)
                .doOnNext(bucketInfo -> throwWhenEmpty(bucketInfo,
                        new MsException(NO_SUCH_BUCKET, "no such bucket. bucket name :" + bucketName + ".")))
                .doOnNext(bucketInfo -> regionCheck(bucketInfo.get(REGION_NAME)))
                .doOnNext(this::siteCheck)
                .doOnNext(bucketInfo -> versionId[0] = StringUtils.isNotEmpty(request.getParam(VERSIONID)) ? request.getParam(VERSIONID)
                        : bucketInfo.containsKey(BUCKET_VERSION_STATUS) ? "" : "null")
                .doOnNext(bucketInfo -> {
                    currentSnapshotMark[0] = bucketInfo.get(CURRENT_SNAPSHOT_MARK);
                    snapshotLink[0] = bucketInfo.get(SNAPSHOT_LINK);
                })
                .map(bucketInfo -> {
                    if (StringUtils.isEmpty(bucketInfo.get("fsid"))
                            && !"1".equals(bucketInfo.get("nfs"))
                            && !"1".equals(bucketInfo.get("cifs"))
                            && !"1".equals(bucketInfo.get("ftp"))) {
                        throw new MsException(INVALID_ARGUMENT, "the bucket does not support file sharing.");
                    }

                    return bucketInfo;
                })
                .flatMap(bucketInfo -> storagePool.mapToNodeInfo(bucketVnode).zipWith(Mono.just(bucketInfo)))
                .flatMap(tuple2 -> ErasureClient.getObjectMetaVersion(bucketName, objName, versionId[0], tuple2.getT1(), request, currentSnapshotMark[0], snapshotLink[0])
                        .doOnNext(metaData -> checkMetaData(objName, metaData, request.getParam(VERSIONID), currentSnapshotMark[0]))
                        .zipWith(Mono.just(tuple2.getT2())))
                .map(tuple2 -> {
                    MetaData metaData = tuple2.getT1();
                    Inode inode = null;
                    try {
                        if (metaData.inode > 0 && StringUtils.isNotBlank(metaData.getTmpInodeStr())) {
                            inode = Json.decodeValue(metaData.getTmpInodeStr(), Inode.class);
                        }
                    } catch (Exception e) {
                        logger.error("get fs acl info error ", e);
                    }

                    if (null == inode || InodeUtils.isError(inode)) {
                        throw new MsException(UNKNOWN_ERROR, "inode is not exist.");
                    } else if (StringUtils.isBlank(metaData.getObjectAcl())) {
                        throw new MsException(UNKNOWN_ERROR, "objAcl is not exist.");
                    }

                    if (null == inode.getObjAcl() && StringUtils.isNotBlank(metaData.getObjectAcl())) {
                        inode.setObjAcl(metaData.getObjectAcl());
                    }

                    return inode;
                })
                .map(inode -> {
                    FsACL fsACL = new FsACL();
                    fsACL.setObj(inode.getObjName());
                    fsACL.setNodeId(inode.getNodeId());
                    fsACL.setOwner(inode.getUid());
                    fsACL.setGroup(inode.getGid());

                    String s3Owner = ACLUtils.getS3IdByUid(inode.getUid());
                    Set<Integer> gids = ACLUtils.getGids(s3Owner);
                    fsACL.setGids(new LinkedList<>(gids));

                    fsACL.setMode(inode.getMode());
                    List<FsACE> aceList = new LinkedList<>();

                    if (nfsV3[0]) {
                        //UGO权限对应的ACE
                        List<Inode.ACE> list = NFSACL.parseACLtoNfsACL(inode);
                        for (Inode.ACE ace : list) {
                            aceList.add(FsACE.mapToNfsAce(ace));
                        }
                    }

                    if (cifs[0]) {
                        List<Inode.ACE> list = CIFSACL.parseAclToCifsACL(inode);
                        for (Inode.ACE ace : list) {
                            aceList.add(FsACE.mapToCifsAce(ace));
                        }
                    }

                    fsACL.setACEs(aceList);
                    return JaxbUtils.toByteArray(fsACL);
                })
                .subscribe(data -> {
                    //response返回
                    addPublicHeaders(request, getRequestId())
                            .putHeader(CONTENT_TYPE, "application/xml")
                            .putHeader(CONTENT_LENGTH, String.valueOf(data.length))
                            .write(Buffer.buffer(data));
                    addAllowHeader(request.response()).end();
                }, e -> dealException(request, e));
        Optional.ofNullable(request).ifPresent(r -> r.addResponseCloseHandler(v -> subscribe.dispose()));
        return ErrorNo.SUCCESS_STATUS;
    }

    /**
     * 判断协议是否支持
     * @param proto 协议
     * @return boolean 是否
     **/
    public static boolean isProtoSupport(String proto, boolean[] nfsV3, boolean[] cifs) {
        boolean res = false;
        try {
            int protoFlag = Integer.parseInt(proto);
            if ((protoFlag & FsConstants.ProtoFlag.NFSV3_START) != 0) {
                res = true;
                nfsV3[0] = true;
            }

            if ((protoFlag & FsConstants.ProtoFlag.CIFS_START) != 0) {
                res = true;
                cifs[0] = true;
            }
        } catch (Exception e) {
            logger.error("proto error", e);
            res = false;
        }

        return res;
    }
}

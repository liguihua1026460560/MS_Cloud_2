package com.macrosan.action.managestream;

import com.macrosan.action.core.BaseService;
import com.macrosan.constants.ErrorNo;
import com.macrosan.doubleActive.DoubleActiveUtil;
import com.macrosan.filesystem.FsUtils;
import com.macrosan.filesystem.quota.FSQuotaRealService;
import com.macrosan.filesystem.utils.FSQuotaUtils;
import com.macrosan.message.jsonmsg.FSQuotaConfig;
import com.macrosan.message.mqmessage.ResponseMsg;
import com.macrosan.utils.msutils.MsException;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import reactor.core.publisher.MonoProcessor;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import static com.macrosan.constants.ErrorNo.*;
import static com.macrosan.constants.ServerConstants.BUCKET_NAME;
import static com.macrosan.constants.ServerConstants.USER_ID;
import static com.macrosan.constants.SysConstants.*;
import static com.macrosan.doubleActive.DoubleActiveUtil.notifySlaveSite;
import static com.macrosan.filesystem.FsConstants.S_IFDIR;
import static com.macrosan.filesystem.FsConstants.S_IFMT;
import static com.macrosan.filesystem.quota.FSQuotaConstants.*;
import static com.macrosan.filesystem.utils.FSQuotaUtils.checkQuotaTypeAndId;
import static com.macrosan.message.jsonmsg.Inode.ERROR_INODE;
import static com.macrosan.message.jsonmsg.Inode.NOT_FOUND_INODE;

@Log4j2
public class FsQuotaService extends BaseService {

    private static FsQuotaService instance = null;

    private FsQuotaService() {
        super();
    }

    public static FsQuotaService getInstance() {
        if (instance == null) {
            instance = new FsQuotaService();
        }
        return instance;
    }

    private static class ErrorInfo {
        @Getter
        private final int errorNo;
        private final String messageTemplate;

        public ErrorInfo(int errorNo, String messageTemplate) {
            this.errorNo = errorNo;
            this.messageTemplate = messageTemplate;
        }

        public String getMessage(String bucketName, String dirName) {
            return String.format(messageTemplate, bucketName, dirName);
        }
    }

    // 集中管理错误码和对应的异常信息
    private static final Map<Integer, ErrorInfo> ERROR_MAP = new HashMap<>();

    static {
        ERROR_MAP.put(-1, new ErrorInfo(ErrorNo.UNKNOWN_ERROR, "put FsDirQuotaInfo fail. bucket_name: %s, dirName: %s"));
        ERROR_MAP.put(-2, new ErrorInfo(ErrorNo.DIR_NAME_NOT_FOUND, "put FsUserQuotaInfo fail, dir not found. bucket_name: %s, dirName: %s"));
        ERROR_MAP.put(-3, new ErrorInfo(QUOTA_CREATE_FREQUENTLY, "del and create dir quota frequently."));
        ERROR_MAP.put(-4, new ErrorInfo(QUOTA_INFO_NOT_EXITS, "bucket: %s, dirName: %s does not have quota info"));
        ERROR_MAP.put(-5, new ErrorInfo(QUOTA_INFO_LESS_THAN_USED, "bucket: %s, dirName: %s quota info is less than used"));
        ERROR_MAP.put(-6, new ErrorInfo(QUOTA_DIR_NAME_IS_RENAMING, "bucket: %s, dirName: %s ,dir is renaming."));
        ERROR_MAP.put(-7, new ErrorInfo(DIR_NAME_IS_SETTING, "bucket: %s, dirName: %s ,dir is setting quota."));
    }

    public ResponseMsg putFsDirQuotaInfo(Map<String, String> paramMap) {
        paramMap.put(QUOTA_TYPE, String.valueOf(FS_DIR_QUOTA));
        paramMap.put(MSG_TYPE, MSG_TYPE_PUT_FS_DIR_QUOTA_INFO);
        return putFsQuotaInfo(paramMap);
    }

    public ResponseMsg putFsUserQuotaInfo(Map<String, String> paramMap) {
        paramMap.put(QUOTA_TYPE, String.valueOf(FS_USER_QUOTA));
        paramMap.put(MSG_TYPE, MSG_TYPE_PUT_FS_USER_QUOTA_INFO);
        return putFsQuotaInfo(paramMap);
    }

    public ResponseMsg putFsGroupQuotaInfo(Map<String, String> paramMap) {
        paramMap.put(QUOTA_TYPE, String.valueOf(FS_GROUP_QUOTA));
        paramMap.put(MSG_TYPE, MSG_TYPE_PUT_FS_GROUP_QUOTA_INFO);
        return putFsQuotaInfo(paramMap);
    }


    public ResponseMsg putFsQuotaInfo(Map<String, String> paramMap) {
        log.info("putFsQuotaInfo paramMap: {}", paramMap);
        String bucketName = paramMap.get(BUCKET_NAME);
        String userId = paramMap.get(USER_ID);
        Map<String, String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
        if (bucketInfo.isEmpty()) {
            throw new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }
        DoubleActiveUtil.siteConstraintCheck(bucketInfo, paramMap.containsKey(SITE_FLAG) || paramMap.containsKey(SITE_FLAG.toLowerCase()));
        if (!userId.equals(bucketInfo.get(BUCKET_USER_ID))) {
            throw new MsException(ErrorNo.ACCESS_FORBIDDEN,
                    "no permission.user " + userId + " can not configure " + bucketName + " worm.");
        }
        if (!bucketInfo.containsKey("fsid")) {
            throw new MsException(ErrorNo.QUOTA_BUCKET_NOT_START_FS,
                    "no permission.bucket " + bucketName + " do not start FS");
        }
        String msgType = paramMap.get(MSG_TYPE);
        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!DoubleActiveUtil.dealSiteSyncRequest(new UnifiedMap<>(paramMap), msgType, localCluster, masterCluster)) {
            return new ResponseMsg(ErrorNo.SUCCESS_STATUS, new byte[0]);
        }

        String s3AccountName = pool.getCommand(REDIS_USERINFO_INDEX).hget(userId, "name");
        FSQuotaConfig fsQuotaConfig = FSQuotaUtils.buildQuotaConfig(paramMap, bucketName, s3AccountName);
        MonoProcessor<Integer> putRes = MonoProcessor.create();
        String[] errorMsg = new String[]{""};
        FSQuotaRealService.setFsQuotaInfo(fsQuotaConfig, bucketInfo)
                .doOnError(e -> {
                    if (e instanceof MsException) {
                        errorMsg[0] = e.getMessage();
                        putRes.onNext(((MsException) e).getErrCode());
                    }
                    putRes.onNext(-1);
                })
                .defaultIfEmpty(-1)
                .subscribe(putRes::onNext);
        int finalRes = putRes.block();
        ErrorInfo errorInfo = ERROR_MAP.get(finalRes);
        if (errorInfo != null) {
            throw new MsException(errorInfo.getErrorNo(), errorInfo.getMessage(bucketName, fsQuotaConfig.getDirName()));
        }
        if (finalRes != 0) {
            throw new MsException(finalRes, errorMsg[0]);
        }
        String action = ACTION_PUT_FS_DIR_QUOTA_INFO;
        if (MSG_TYPE_PUT_FS_USER_QUOTA_INFO.equals(msgType)) {
            action = ACTION_PUT_FS_USER_QUOTA_INFO;
        } else if (MSG_TYPE_PUT_FS_GROUP_QUOTA_INFO.equals(msgType)) {
            action = ACTION_PUT_FS_GROUP_QUOTA_INFO;
        }

        int resCode = notifySlaveSite(new UnifiedMap<>(paramMap), action);
        if (resCode != SUCCESS_STATUS) {
            throw new MsException(resCode, "master delete bucket error");
        }
        return new ResponseMsg(ErrorNo.SUCCESS_STATUS, new byte[0]);
    }

    public ResponseMsg delFsQuotaInfo(Map<String, String> paramMap) {
        log.info("delFsQuotaInfo paramMap: {}", paramMap);
        String bucketName = paramMap.get(BUCKET_NAME);
        String dirName = paramMap.get(DIR_NAME);
        if (StringUtils.isBlank(dirName)) {
            throw new MsException(INVALID_ARGUMENT, "the dirName input is invalid.");
        }
        try {
            dirName = URLDecoder.decode(dirName, "UTF-8");
        } catch (UnsupportedEncodingException e) {
        }
        String userId = paramMap.get(USER_ID);
        if ("/".equals(dirName)) {
            throw new MsException(INVALID_ARGUMENT, "the dirName input is invalid.");
        }
        Map<String, String> bucketInfo = pool.getCommand(REDIS_BUCKETINFO_INDEX).hgetall(bucketName);
        if (bucketInfo.isEmpty()) {
            throw new MsException(ErrorNo.NO_SUCH_BUCKET, "no such bucket. bucket_name: " + bucketName);
        }
        if (!userId.equals(bucketInfo.get(BUCKET_USER_ID))) {
            throw new MsException(ErrorNo.ACCESS_FORBIDDEN,
                    "no permission.user " + userId + " can not configure " + bucketName + " worm.");
        }
        if (!bucketInfo.containsKey("fsid")) {
            throw new MsException(ErrorNo.QUOTA_BUCKET_NOT_START_FS,
                    "no permission.bucket " + bucketName + " do not start FS");
        }

        String localCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(LOCAL_CLUSTER, CLUSTER_NAME);
        String masterCluster = pool.getCommand(REDIS_SYSINFO_INDEX).hget(MASTER_CLUSTER, CLUSTER_NAME);
        if (!DoubleActiveUtil.dealSiteSyncRequest(new UnifiedMap<>(paramMap), MSG_TYPE_DEL_FS_QUOTA_INFO, localCluster, masterCluster)) {
            return new ResponseMsg(ErrorNo.SUCCESS_STATUS, new byte[0]);
        }

        if (!dirName.endsWith("/")) {
            dirName += "/";
        }
        MonoProcessor<Integer> delRes = MonoProcessor.create();
        try {
            int[] id_ = new int[]{-1};
            int[] quotaType = new int[]{-1};
            checkQuotaTypeAndId(paramMap.get(QUOTA_TYPE), paramMap.get(UID), quotaType, id_);

            FsUtils.lookup(bucketName, dirName, null, false, false, -1, null)
                    .onErrorReturn(ERROR_INODE)
                    .subscribe(inode -> {
                        if (inode.getLinkN() == NOT_FOUND_INODE.getLinkN()) {
                            delRes.onNext(-2);
                            return;
                        }
                        if (inode.getLinkN() == ERROR_INODE.getLinkN()) {
                            delRes.onNext(-1);
                            return;
                        }
                        if ((inode.getMode() & S_IFMT) != S_IFDIR) {
                            delRes.onNext(-2);
                            return;
                        }
                        FSQuotaRealService.delFsDirQuotaInfo(bucketName, inode.getNodeId(), inode.getObjName(), quotaType[0], id_[0], delRes, bucketInfo.get("user_name"), false);
                    });

        } catch (Exception ex) {
            throw new MsException(ErrorNo.INVALID_ARGUMENT, "del FsQuotaInfo args error. bucket_name: " + bucketName + ",dirName:" + dirName);
        }

        String[] errorMsg = new String[]{""};
        int finalRes = delRes
                .doOnError(e -> {
                    if (e instanceof MsException) {
                        errorMsg[0] = e.getMessage();
                        delRes.onNext(((MsException) e).getErrCode());
                    }
                    delRes.onNext(-1);
                })
                .block();

        ErrorInfo errorInfo = ERROR_MAP.get(finalRes);
        if (errorInfo != null) {
            throw new MsException(errorInfo.getErrorNo(), errorInfo.getMessage(bucketName, dirName));
        }
        if (finalRes != 0) {
            throw new MsException(finalRes, errorMsg[0]);
        }

        int resCode = notifySlaveSite(new UnifiedMap<>(paramMap), ACTION_DEL_FS_QUOTA_INFO);
        if (resCode != SUCCESS_STATUS) {
            throw new MsException(resCode, "master delete bucket error");
        }
        return new ResponseMsg(ErrorNo.SUCCESS_STATUS, new byte[0]);
    }


}

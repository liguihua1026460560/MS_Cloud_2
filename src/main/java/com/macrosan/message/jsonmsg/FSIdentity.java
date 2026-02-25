package com.macrosan.message.jsonmsg;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;

/**
 * @Author: WANG CHENXING
 * @Date: 2025/6/5
 * @Description:
 */
@Data
@Accessors(chain = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@Log4j2
public class FSIdentity {
    public static final String USER_SID_PREFIX = "S-1-5-";
    public static final String GROUP_SID_PREFIX = "S-1-6-";

    //s3：判断s3权限使用；即桶acl和对象acl
    String s3Id;

    //nfs：判断nfs权限使用；即ugo权限和nfs acl权限
    int uid;
    int gid;

    //cifs: 判断cifs权限使用；即cifs acl权限；实质上还是通过uid和gid来判断，因为userSid和groupSid实际由uid和gid生成
    String userSid;
    String groupSid;

    //sysMeta中的展示名
    String displayName;

    public FSIdentity(String s3Id, int uid, int gid, String userSid, String groupSid) {
        this.s3Id = s3Id;
        this.uid = uid;
        this.gid = gid;
        this.userSid = userSid;
        this.groupSid = groupSid;
    }

    public FSIdentity(String s3Id, int uid, int gid, String userSid, String groupSid, String s3Name) {
        this.s3Id = s3Id;
        this.uid = uid;
        this.gid = gid;
        this.userSid = userSid;
        this.groupSid = groupSid;
        this.displayName = s3Name;
    }

    public static String getUserSIDByUid(int uid) {
        StringBuilder builder = new StringBuilder();
        builder.append(USER_SID_PREFIX);
        builder.append(uid);
        return builder.toString();
    }

    public static String getGroupSIDByGid(int gid) {
        StringBuilder builder = new StringBuilder();
        builder.append(GROUP_SID_PREFIX);
        builder.append(gid);
        return builder.toString();
    }

    /**
     * 根据SID获取uid
     **/
    public static int getUidBySID(String userSID) {
        int uid = -1;
        try {
            String uidStr = userSID.substring(USER_SID_PREFIX.length());
            uid = Integer.parseInt(uidStr);
        } catch (Exception e) {
            log.error("get uid by SID error: {}", userSID);
        }

        return uid;
    }

    /**
     * 根据SID获取gid
     **/
    public static int getGidBySID(String groupSID) {
        int uid = -1;
        try {
            String gidStr = groupSID.substring(GROUP_SID_PREFIX.length());
            uid = Integer.parseInt(gidStr);
        } catch (Exception e) {
            log.error("get gid by SID error: {}", groupSID);
        }

        return uid;
    }

    public FSIdentity cloneIdentity() {
        FSIdentity identity = new FSIdentity(this.s3Id, this.uid, this.gid, this.userSid, this.groupSid, this.displayName);
        return identity;
    }
}

package com.macrosan.message.jsonmsg;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @Author DaiFengTao
 * @Date 2025/7/9 10:20
 * @PackageName:com.macrosan.message.jsonmsg
 * @ClassName: FSIpACL
 * @Description: TODO
 **/

@Data
@Accessors(chain = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class FSIpACL {


    /**
     * 所设置的网段、ip信息
     * ip：192.168.1.1
     * 网段1：192.168.1.0/24
     * ip范围：192.168.1.1-111
     * 网段3:192.*
     */
    public String ipInfo;
    /**
     * 参照MOFS预留字段，暂不使用
     * rw:读写（默认）
     * ro:只读
     */
    public String optAccess;

    /**
     * 参照MOFS预留字段，暂不使用
     * sync：同步写入
     * async：异步写入（默认）
     */
    public String writeMode;

    /**
     * no_root_squash: root用户访问时，将被映射成root用户，具备root权限（默认）
     * root_squash: root用户访问时，将被映射成匿名用户 uid=65534，具备other 权限
     * all_squash: 所有用户访问时，将被映射成匿名用户，具备other 权限
     */
    public String squash;

    /**
     * 匿名用户uid 默认65534
     */
    public int anonuid;

    /**
     * 匿名用户gid 默认65534
     */
    public int anongid;

    /**
     * 参照MOFS预留字段，暂不使用
     * insecure:不校验端口（默认）
     * secure:校验端口
     */
    public String portMode;


    public static final FSIpACL DEFAULT_FS_IP_ACL = new FSIpACL()
            .setIpInfo("*")
            .setOptAccess("rw")
            .setWriteMode("async")
            .setSquash("no_root_squash")
            .setAnonuid(65534)
            .setAnongid(65534)
            .setPortMode("insecure");

    public FSIpACL clone() {
        return new FSIpACL()
                .setIpInfo(ipInfo)
                .setOptAccess(optAccess)
                .setWriteMode(writeMode)
                .setSquash(squash)
                .setAnonuid(anonuid)
                .setAnongid(anongid)
                .setPortMode(portMode);
    }
}

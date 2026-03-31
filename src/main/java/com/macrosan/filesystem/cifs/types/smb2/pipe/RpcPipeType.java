package com.macrosan.filesystem.cifs.types.smb2.pipe;

import com.macrosan.message.jsonmsg.Inode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

import static com.macrosan.message.jsonmsg.Inode.*;

@Slf4j
public enum RpcPipeType {
    LSARPC("lsarpc", "lsarpc", -15, LSARPC_INODE, "\\PIPE\\lsarpc"),
    WKSSVC("wkssvc", "wkssvc", -16, WKSSVC_INODE, "\\PIPE\\wkssvc"),
    SAMR("samr", "samr", -17, SAMR_INODE, "\\PIPE\\samr");
    //DSSETUP("dssetup", "/lsarpc", -18, DSSETUP_INODE, "\\PIPE\\dssetup");

    private final String name;
    private final String objName;
    private final int linkN;
    private final Inode parentInode;
    private final String scndryAddr;

    public String getName() {
        return name;
    }

    public String getObjName() {
        return objName;
    }

    public int getLinkN() {
        return linkN;
    }

    public Inode getParentInode() {
        return parentInode;
    }

    public String getScndryAddr() { return scndryAddr;}

    RpcPipeType(String name, String objName, int linkN, Inode parentInode, String scndryAddr) {
        this.name = name;
        this.objName = objName;
        this.linkN = linkN;
        this.parentInode = parentInode;
        this.scndryAddr = scndryAddr;
    }

    public static Optional<RpcPipeType> fromFileName(String fileName) {
        if (fileName == null) return Optional.empty();
        String lower = fileName.toLowerCase();
        for (RpcPipeType type : values()) {
            if (type.name.equals(lower)) {
                return Optional.of(type);
            }
        }
        return Optional.empty();
    }

    public static Optional<RpcPipeType> fromLinkN(int linkN) {

        for (RpcPipeType type : values()) {
            if (type.linkN == linkN) {
                return Optional.of(type);
            }
        }
        return Optional.empty();
    }

    public static Optional<RpcPipeType> fromObjName(String objName) {
        if (objName == null) return Optional.empty();
        for (RpcPipeType type : values()) {
            if (type.objName.equals(objName)) {
                return Optional.of(type);
            }
        }
        return Optional.empty();
    }

    /**
     * 判断是否是管道类型
     *
     * 已知的管道类型返回 0
     * 未知的管道类型返回 1
     * 不是管道类型返回  -1
     * */
    public static int judgePipe(String obj, int tid) {
        int res = -1;
        if (tid == -1 && StringUtils.isNotBlank(obj)) {
            Optional<RpcPipeType> rpcTypeOpt = RpcPipeType.fromFileName(obj);
            if (rpcTypeOpt.isPresent()) {
                //已知的三种
                res = 0;
            } else {
                //srvsvc等未知的管道
                res = 1;
            }
        }

        return res;
    }
}

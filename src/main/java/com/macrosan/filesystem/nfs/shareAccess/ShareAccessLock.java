package com.macrosan.filesystem.nfs.shareAccess;

import com.macrosan.filesystem.lock.Lock;
import com.macrosan.filesystem.nfs.delegate.DelegateLock;
import com.macrosan.filesystem.nfs.types.StateId;
import com.macrosan.httpserver.ServerConfig;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Arrays;
import java.util.Objects;

import static com.macrosan.filesystem.nfs.api.NFS4Proc.clientControl;

@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
@Data
public class ShareAccessLock extends Lock {
    public String bucket = "";
    public String objName;
    public long nodeId;
    public StateId stateId = new StateId();
    public int shareAccess;
    public int shareDeny;
    public String node= "";
    public long clientId;
    public byte[] sessionId = new byte[0];
    public byte[] owner = new byte[0];
    public int type;
    public int stateIdType;
    public String versionNum;
    public boolean init;

    //    public static final ShareAccessLock EMPTY_SHARE = new ShareAccessLock().setShareAccess(0);
    public static final ShareAccessLock ERROR_SHARE = new ShareAccessLock().setShareAccess(-1).setVersionNum("")
            .setBucket("").setObjName("").setNode("").setClientId(-1).setOwner(new byte[0]);
    public static final ShareAccessLock CONFLICT_SHARE = new ShareAccessLock().setShareAccess(-2).setVersionNum("a")
            .setBucket("").setObjName("").setNode("").setClientId(-2).setOwner(new byte[0]);
    public static final ShareAccessLock NOT_FOUND_SHARE = new ShareAccessLock().setShareAccess(-3).setVersionNum("")
            .setBucket("").setObjName("").setNode("").setClientId(-3).setOwner(new byte[0]);
    public static final int ADD_SHARE_TYPE = 0;
    public static final int REMOVE_ADD_SHARE_TYPE = 1;
    public static final int EXIST_SHARE_TYPE = 2;
    public static final int GET_SHARE_TYPE = 3;
    public static final int OPEN_SHARE_CONFLICT_TYPE = 4;
    public static final int OPEN_DENY_CONFLICT_TYPE = 5;




    @Override
    public boolean needKeep() {
        return node.equals(ServerConfig.getInstance().getHostUuid())
                && clientControl.getClient0(clientId) != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShareAccessLock that = (ShareAccessLock) o;
        return nodeId == that.nodeId && clientId == that.clientId && bucket.equals(that.bucket) && node.equals(that.node) && Arrays.equals(owner, that.owner);
    }

    public ShareAccessLock clone(){
        return new ShareAccessLock()
                .setBucket(bucket).setObjName(objName).setNodeId(nodeId).setStateId(stateId)
                .setShareAccess(shareAccess).setShareDeny(shareDeny).setNode(node).setClientId(clientId)
                .setSessionId(sessionId).setOwner(owner).setType(type).setStateIdType(stateIdType).setVersionNum(versionNum).setInit(init);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(bucket, nodeId, node, clientId);
        result = 31 * result + Arrays.hashCode(owner);
        return result;
    }

}

package com.macrosan.filesystem.cifs.notify;

import com.macrosan.filesystem.cifs.handler.SMBHandler;
import com.macrosan.filesystem.cifs.reply.smb2.NotifyReply;
import com.macrosan.filesystem.lock.Lock;
import com.macrosan.httpserver.ServerConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;


@EqualsAndHashCode(callSuper = false, exclude = "task")
@Data
@NoArgsConstructor
public class NotifyLock extends Lock {
    boolean tree;
    long sessionID;
    long asyncID;
    int filter;
    String node;
    String version;
    NotifyTask task;

    public NotifyLock(boolean tree, long sessionID, long asyncID, int filter, String node, String version) {
        this.tree = tree;
        this.sessionID = sessionID;
        this.asyncID = asyncID;
        this.filter = filter;
        this.node = node;
        this.version = version;
    }

    @Override
    //只支持SMB2
    //在session关闭后自动不进行保活
    public boolean needKeep() {
        return node.equals(ServerConfig.getInstance().getHostUuid()) && SMBHandler.session2Map.containsKey(sessionID);
    }

    public void cancel() {
        if (node.equals(ServerConfig.getInstance().getHostUuid())) {
            NotifyCache cache = NotifyCache.clearByAsync(asyncID);
            if (cache != null && task != null) {
                NotifyReply reply = new NotifyReply();

                for (int i = 0; i < task.action.length; i++) {
                    NotifyReply.NotifyInfo info = new NotifyReply.NotifyInfo();
                    info.action = task.action[i];
                    info.fileName = task.notifyKey[i].substring(cache.key.length());
                    if (info.fileName.endsWith("/")) {
                        info.fileName = info.fileName.substring(0, info.fileName.length() - 1);
                    }

                    reply.infoList.add(info);
                    
                    if (i == task.action.length - 1) {
                        info.next = 0;
                    } else {
                        info.next = info.size();
                    }
                }

                cache.handler.changNotify(cache.replyHeader, reply);
            }
        }
    }
}

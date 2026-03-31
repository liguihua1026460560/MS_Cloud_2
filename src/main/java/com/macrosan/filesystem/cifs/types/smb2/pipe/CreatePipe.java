package com.macrosan.filesystem.cifs.types.smb2.pipe;

import com.macrosan.message.jsonmsg.Inode;


public class CreatePipe {
    public static Inode createVirtualRpcPipeInode(RpcPipeType pipeType) {
        Inode rpcInode = new Inode();
        long rpcNodeId = -1;
        rpcInode.setNodeId(rpcNodeId);
        rpcInode.setObjName(pipeType.getName());
        rpcInode.setLinkN(pipeType.getLinkN());
        rpcInode.setBucket("pipe："+pipeType.name());

        return rpcInode;
    }
}

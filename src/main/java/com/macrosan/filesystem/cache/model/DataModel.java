package com.macrosan.filesystem.cache.model;

import com.macrosan.filesystem.cache.InodeOperator;
import com.macrosan.message.jsonmsg.Inode;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

public interface DataModel {
    void append(List<Inode.InodeData> list, Inode.InodeData data, Inode inode, long reqOffset);

    void updateInodeData(long reqOffset, Inode.InodeData data, Inode inode, InodeOperator.UpdateArgs args, String oldInodeData);

    Mono<Inode> updateChunk(Inode inode, Map<String, List<Inode.InodeData>> needDelete);
}

package com.macrosan.filesystem.cifs.types.smb2;

import com.macrosan.filesystem.cifs.SMB2;
import lombok.Data;
import lombok.experimental.Accessors;
import reactor.core.publisher.Mono;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

//存储SMB2 Compound请求中共用的字段
@Data
@Accessors(chain = true)
public class CompoundRequest {
    SMB2FileId fileId;
    public int execNum = 0;
    public List<Mono<SMB2.SMB2Reply>> replyList = new LinkedList<>();
    public AtomicInteger maxMsgBuf;
}

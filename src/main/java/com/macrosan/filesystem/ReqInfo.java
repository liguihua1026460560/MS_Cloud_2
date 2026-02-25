package com.macrosan.filesystem;

import com.macrosan.filesystem.nfs.handler.NFSHandler;
import com.macrosan.filesystem.nfs.handler.NLMHandler;
import com.macrosan.filesystem.nfs.handler.NSMHandler;
import com.macrosan.message.jsonmsg.FSIpACL;
import lombok.ToString;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


@ToString(exclude = {"bucketInfo", "lock"})
public class ReqInfo {
    public String bucket;
    public NFSHandler nfsHandler;
    public NLMHandler nlmHandler;
    public NSMHandler nsmHandler;
    public Map<String, String> bucketInfo;
    public Map<String, String> lock = new ConcurrentHashMap<>();
    public FSIpACL ipACL;
    public boolean optCompleted = true;
    public boolean repeat = false;
    public boolean timeout = false;
}

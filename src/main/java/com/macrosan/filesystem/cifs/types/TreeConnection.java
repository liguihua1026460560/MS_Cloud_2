package com.macrosan.filesystem.cifs.types;

import com.macrosan.filesystem.cifs.SMB2;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class TreeConnection {
    public String bucket;
    public Map<String, String> bucketInfo;
    public int tid;

    public TreeConnection(String bucket, Map<String, String> bucketInfo, int tid) {
        this.bucket = bucket;
        this.bucketInfo = bucketInfo;
        this.tid = tid;
    }

    private static final Map<String, String> IPC_MAP = new HashMap<>();
    public static final TreeConnection IPC_TCON = new TreeConnection(SMB2.IPC, TreeConnection.IPC_MAP, -1);
}

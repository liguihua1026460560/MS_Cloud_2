package com.macrosan.ec.rebuild;

import com.dslplatform.json.JsonAttribute;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.macrosan.storage.StoragePool;
import lombok.Data;
import lombok.experimental.Accessors;
import reactor.core.publisher.MonoProcessor;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * 重建任务
 *
 * @author gaozhiyuan
 */
@Data
@Accessors(chain = true)
public class ReBuildTask {
    enum Type {
        OBJECT_META,
        OBJECT_FILE,
        INIT_PART_UPLOAD,
        PART_UPLOAD,
        STATISTIC,
        BUCKET_STORAGE,
        SYNC_RECORD,
        DEDUP_INFO,
        COMPONENT_RECORD,
        NFS_INODE,
        NFS_CHUNK,
        STS_TOKEN,
        AGGREGATE_META
    }

    @JsonAttribute
    public int retryNums = 0;
    @JsonAttribute
    public MonoProcessor<Integer> res;
    @JsonAttribute
    public Type type;
    @JsonAttribute
    public String vnode;
    @JsonAttribute
    public String disk;
    @JsonAttribute
    public String pool;
    @JsonAttribute
    public String[] diskLink;
    @JsonAttribute
    public Map<String, String> map;
    @JsonIgnore
    public CopyObjectFileRebuilder copyObjectFileRebuilder;
    public ReBuildTask() {

    }

    public ReBuildTask(Type type, String disk, String vnode, String[] diskLink, StoragePool pool, MonoProcessor<Integer> res) {
        this.type = type;
        this.disk = disk;
        this.vnode = vnode;
        this.diskLink = diskLink;
        this.pool = pool.getVnodePrefix();
        this.res = res;
        map = new LinkedHashMap<>();
    }

    public ReBuildTask(Type type, String disk, String vnode, String[] diskLink, String pool, MonoProcessor<Integer> res, Map<String, String> map) {
        this.type = type;
        this.disk = disk;
        this.vnode = vnode;
        this.diskLink = diskLink;
        this.pool = pool;
        this.res = res;
        this.map = new LinkedHashMap<>(map);
    }

}

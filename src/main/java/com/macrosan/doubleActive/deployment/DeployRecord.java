package com.macrosan.doubleActive.deployment;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.macrosan.message.jsonmsg.MetaData;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 异步复制/双活部署新站点时，记录同步信息
 */
@Data
@CompiledJson
@Accessors(chain = true)
public class DeployRecord {
    @JsonAttribute
    public int clusterIndex;

    /**
     * 保存非主站点的添加时间戳。
     */
    @JsonAttribute
    public long timeStamp;

    /**
     * 扫描object metadata时，记录各个桶下当前已扫描到并没有处理完的metaData信息。[bucketName, [bucketName, metaData.key, metaData.versionId, metaData.stamp]]
     * 归档的格式为[bucketName_bucketVnode, [bucketName, metaData.key, metaData.versionId, metaData.stamp]]
     */
    @JsonAttribute
    public Map<String, String[]> objMarkerMap = new ConcurrentHashMap<>();

    /**
     * [bucket, [rocksKey, MetaData]]，value按字典排序
     * 归档的格式为[bucketName_bucketVnode, [rocksKey, MetaData]]
     */
    @JsonIgnore
    public Map<String, Map<String, MetaData>> dealingObjMap = new ConcurrentHashMap<>();

    /**
     * 扫描part_multi_list时，记录各个桶下当前已扫描到并没有处理完的Upload的key。[bucket, partKey]
     */
    @JsonAttribute
    public Map<String, String> partMarkerMap = new ConcurrentHashMap<>();

    /**
     * [bucket, [uploadId, partKey]]
     */
    @JsonIgnore
    public Map<String, Map<String, String>> dealingPartMap = new ConcurrentHashMap<>();

    /**
     * [bucket, rocksKey]
     */
    @JsonAttribute
    public Map<String, String> recordMarkerMap = new ConcurrentHashMap<>();

    /**
     * [bucket, [rocksKey, 没用]]
     */
    @JsonIgnore
    public Map<String, Map<String, String>> dealingRecordMap = new ConcurrentHashMap<>();


    /**
     * [bucket, rocksKey]
     */
    @JsonAttribute
    public Map<String, String> oldRecordMarkerMap = new ConcurrentHashMap<>();

    /**
     * [bucket, [rocksKey, 没用]]
     */
    @JsonIgnore
    public Map<String, Map<String, String>> dealingOldRecordMap = new ConcurrentHashMap<>();

    public DeployRecord() {
    }


}

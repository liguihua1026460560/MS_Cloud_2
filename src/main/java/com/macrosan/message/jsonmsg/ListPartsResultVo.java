package com.macrosan.message.jsonmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * describe:列出指定uploadId下分片任务的对象
 *
 * @author chengyinfeng
 * @date 2019/01/03
 */
@Data
@CompiledJson
public class ListPartsResultVo {
    /**
     * bucket名称
     **/
    @JsonAttribute
    public String bucket;

    /**
     * 对象的key名称
     **/
    @JsonAttribute
    public String key;

    /**
     * 分片任务的uploadId
     **/
    @JsonAttribute
    public String uploadId;

    /**
     * 用户id
     **/
    @JsonAttribute
    public String id;

    /**
     * 用户名称
     **/
    @JsonAttribute
    public String displayName;

    /**
     * 列出part的标志-列出此后的part
     **/
    @JsonAttribute
    public int partNumberMarker;

    /**
     * 标明接下来的PartNumberMarker值
     **/
    @JsonAttribute
    public int nextPartNumberMarker;

    /**
     * 最多列出的parts数目
     **/
    @JsonAttribute
    public int maxParts;

    /**
     * 返回结果是否为列出全部：true-有缩短，未列出全部  false-已列出全部
     **/
    @JsonAttribute
    public boolean truncated;

    /**
     * 符合列出条件的parts
     **/
    @JsonAttribute
    public List<Map<String, String>> partList;
}

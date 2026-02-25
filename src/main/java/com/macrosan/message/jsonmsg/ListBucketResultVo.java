package com.macrosan.message.jsonmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import lombok.Data;

import java.util.List;

/**
 * describe:
 *
 * @author chengyinfeng
 * @date 2019/01/14
 */
@Data
@CompiledJson
public class ListBucketResultVo {
    /**
     * bucket名称
     **/
    @JsonAttribute
    public String bucket;

    /**
     * 匹配该前缀的对象--暂不支持
     **/
    @JsonAttribute
    public String prefix;

    /**
     * 标识符，返回其后的对象--暂不支持
     **/
    @JsonAttribute
    public String marker;

    @JsonAttribute
    public String nextMarker;

    /**
     * 本次请求返回的最多key数目
     **/
    @JsonAttribute
    public int maxKeys;

    /**
     * 返回结果是否为列出全部：true-有缩短，未列出全部  false-已列出全部--暂不支持
     **/
    @JsonAttribute
    public boolean truncated;

    /**
     * Object名称进行分组的字符--暂不支持
     **/
    @JsonAttribute
    public String delimiter;

    /**
     * 本次请求返回的满足条件的对象信息列表
     **/
    @JsonAttribute
    public List<ObjectFileVo> contents;

    /**
     * 带delimiter参数时，保存带commonprefixes信息--暂不支持
     **/
    @JsonAttribute
    public List<String> commonPrefixes;
}

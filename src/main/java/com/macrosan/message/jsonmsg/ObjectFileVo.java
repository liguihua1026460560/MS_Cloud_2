package com.macrosan.message.jsonmsg;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import lombok.Data;

/**
 * describe:
 *
 * @author chengyinfeng
 * @date 2019/01/14
 */
@Data
@CompiledJson
public class ObjectFileVo {
    /**
     * 对象的key
     **/
    @JsonAttribute
    public String key;

    /**
     * 对象的最近一次的修改时间
     **/
    @JsonAttribute
    public String lastModified;

    /**
     * 对象的md5
     **/
    @JsonAttribute
    public String etag;

    /**
     * 对象的size
     **/
    @JsonAttribute
    public String size;

    /**
     * 对象的所属Owner的id
     **/
    @JsonAttribute
    public String id;

    /**
     * 对象的所属Owner的name
     **/
    @JsonAttribute
    public String displayName;
}

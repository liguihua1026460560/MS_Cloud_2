package com.macrosan.message.xmlmsg.section;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Contents
 * 保存返回结果中每个 Object 信息的容器
 *
 * @author liyixin
 * @date 2018/11/12
 */
@Data
@Accessors(chain = true)
@XmlRootElement(name = "Contents")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Contents", propOrder = {
        "key",
        "lastModified",
        "etag",
        "size",
        "storageClass",
        "owner"
})
public class Contents {

    @XmlElement(name = "Key", required = true)
    private String key;

    @XmlElement(name = "LastModified", required = true)
    private String lastModified;

    @XmlElement(name = "ETag", required = true)
    private String etag;

    @XmlElement(name = "Size", required = true)
    private String size;

    @XmlElement(name = "StorageClass", required = true)
    private String storageClass;

    @XmlElement(name = "Owner", required = true)
    private Owner owner;

}

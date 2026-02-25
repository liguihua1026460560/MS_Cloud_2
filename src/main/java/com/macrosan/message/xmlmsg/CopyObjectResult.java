package com.macrosan.message.xmlmsg;


import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Uploads
 * 保存 CopyObjectResult 结果的容器
 *
 * @author wangqiujia
 */
@Data
@Accessors(chain = true)
@XmlRootElement(name = "CopyObjectResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "CopyObjectResult", propOrder = {
        "lastModified",
        "etag",
})

public class CopyObjectResult {

    @XmlElement(name = "LastModified", required = true)
    private String lastModified;

    @XmlElement(name = "ETag", required = true)
    private String etag;

}

package com.macrosan.message.xmlmsg;


import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * CompleteMultipartUploadResult
 * 保存 Complete MultipartUpload 请求结果的容器
 *
 * @author mazhongcheng
 * @date 2021/09/14
 */
@Data
@Accessors(chain = true)
@XmlRootElement(name = "CopyPartResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "CopyPartResult", propOrder = {
        "lastModified",
        "etag",
})

public class CopyPartResult {

    @XmlElement(name = "LastModified", required = true)
    private String lastModified;

    @XmlElement(name = "ETag", required = true)
    private String etag;

}

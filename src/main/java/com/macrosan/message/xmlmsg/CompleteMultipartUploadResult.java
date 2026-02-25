package com.macrosan.message.xmlmsg;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * CompleteMultipartUploadResult
 * 保存 Complete MultipartUpload 请求结果的容器
 *
 * @author chengyinfeng
 * @date 2019/03/14
 */
@Data
@Accessors(chain = true)
@XmlRootElement(name = "CompleteMultipartUploadResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "CompleteMultipartUploadResult", propOrder = {
        "bucket",
        "key",
        "etag"
})
public class CompleteMultipartUploadResult {

    @XmlElement(name = "Bucket", required = true)
    private String bucket;

    @XmlElement(name = "Key", required = true)
    private String key;

    @XmlElement(name = "ETag", required = true)
    private String etag;

}

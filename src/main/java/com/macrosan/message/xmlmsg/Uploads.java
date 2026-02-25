package com.macrosan.message.xmlmsg;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Uploads
 * 保存 InitiateMultipartUpload 结果的容器
 *
 * @author liyixin
 * @date 2018/11/12
 */
@Data
@Accessors(chain = true)
@XmlRootElement(name = "InitiateMultipartUploadResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Uploads", propOrder = {
        "bucket",
        "key",
        "uploadId"
})
public class Uploads {

    @XmlElement(name = "Bucket", required = true)
    private String bucket;

    @XmlElement(name = "Key", required = true)
    private String key;

    @XmlElement(name = "UploadId", required = true)
    private String uploadId;

}

package com.macrosan.message.xmlmsg;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * S3 POST Object 201 响应的 XML 模型
 * <p>
 * 当 success_action_status=201 时返回此 XML 文档
 *
 * <?xml version="1.0" encoding="UTF-8"?>
 * <PostResponse>
 *   <Location>http://Example-Bucket.s3.amazonaws.com/ObjectName</Location>
 *   <Bucket>Example-Bucket</Bucket>
 *   <Key>ObjectName</Key>
 *   <ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>
 * </PostResponse>
 */
@Data
@Accessors(chain = true)
@XmlRootElement(name = "PostResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "PostResponse", propOrder = {
        "location",
        "bucket",
        "key",
        "etag",
})
public class PostObjectResponse {

    @XmlElement(name = "Location", required = true)
    private String location;

    @XmlElement(name = "Bucket", required = true)
    private String bucket;

    @XmlElement(name = "Key", required = true)
    private String key;

    @XmlElement(name = "ETag", required = true)
    private String etag;

}

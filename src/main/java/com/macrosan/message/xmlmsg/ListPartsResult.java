package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.Owner;
import com.macrosan.message.xmlmsg.section.Part;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * ListPartsResult
 * 保存 List Part 请求结果的容器
 *
 * @author liyixin
 * @date 2018/11/13
 */
@Data
@Accessors(chain = true)
@XmlRootElement(name = "ListPartsResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ListPartsResult", propOrder = {
        "bucket",
        "key",
        "uploadId",
        "initiator",
        "owner",
        "storageClass",
        "partNumberMarker",
        "nextPartNumberMarker",
        "maxParts",
        "isTruncated",
        "parts"
})
public class ListPartsResult {

    @XmlElement(name = "Bucket", required = true)
    private String bucket;

    @XmlElement(name = "Key", required = true)
    private String key;

    @XmlElement(name = "UploadId", required = true)
    private String uploadId;

    @XmlElement(name = "Initiator", required = true)
    private Owner initiator;

    @XmlElement(name = "Owner", required = true)
    private Owner owner;

    @XmlElement(name = "StorageClass", required = true)
    private String storageClass = "STANDARD";

    @XmlElement(name = "PartNumberMarker", required = true)
    private int partNumberMarker;

    @XmlElement(name = "NextPartNumberMarker", required = true)
    private int nextPartNumberMarker;

    @XmlElement(name = "MaxParts", required = true)
    private int maxParts;

    @XmlElement(name = "IsTruncated", required = true)
    private boolean isTruncated;

    @XmlElement(name = "Part", required = true)
    private List<Part> parts;
}

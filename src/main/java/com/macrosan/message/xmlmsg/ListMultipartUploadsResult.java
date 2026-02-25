package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.Prefix;
import com.macrosan.message.xmlmsg.section.Upload;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * ListMultipartUploadsResult
 * 保存 List MultipartUploads 请求结果的容器
 *
 * @author liyixin
 * @date 2018/11/13
 */
@Data
@Accessors(chain = true)
@XmlRootElement(name = "ListMultipartUploadsResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ListMultipartUploadsResult", propOrder = {
        "bucket",
        "keyMarker",
        "uploadIdMarker",
        "nextKeyMarker",
        "nextUploadIdMarker",
        "delimiter",
        "prefix",
        "maxUploads",
        "isTruncated",
        "uploads",
        "prefixList"
})
public class ListMultipartUploadsResult {

    @XmlElement(name = "Bucket", required = true)
    private String bucket;

    @XmlElement(name = "KeyMarker", required = true)
    private String keyMarker;

    @XmlElement(name = "UploadIdMarker", required = true)
    private String uploadIdMarker;

    @XmlElement(name = "NextKeyMarker", required = true)
    private String nextKeyMarker;

    @XmlElement(name = "NextUploadIdMarker", required = true)
    private String nextUploadIdMarker;

    @XmlElement(name = "Delimiter", required = true)
    private String delimiter;

    @XmlElement(name = "Prefix", required = true)
    private String prefix;

    @XmlElement(name = "MaxUploads", required = true)
    private int maxUploads;

    @XmlElement(name = "IsTruncated", required = true)
    private boolean isTruncated;

    @XmlElement(name = "Upload", required = true)
    private List<Upload> uploads;

    @XmlElement(name = "CommonPrefixes", required = true)
    private List<Prefix> prefixList;
}

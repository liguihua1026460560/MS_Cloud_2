package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.Contents;
import com.macrosan.message.xmlmsg.section.Prefix;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * ListBucketResult
 * 保存 ListObjects 请求结果的容器
 *
 * @author liyixin
 * @date 2018/11/12
 */
@Data
@Accessors(chain = true)
@XmlRootElement(name = "ListBucketResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ListBucketResult", propOrder = {
        "name",
        "prefix",
        "continuationToken",
        "marker",
        "nextMarker",
        "keyCount",
        "maxKeys",
        "isTruncated",
        "delimiter",
        "startAfter",
        "nextContinuationToken",
        "contents",
        "prefixlist"
})
public class ListBucketResult {

    @XmlElement(name = "Name", required = true)
    private String name;

    @XmlElement(name = "Prefix", required = true)
    private String prefix;

    @XmlElement(name = "Marker", required = true)
    private String marker;

    @XmlElement(name = "NextMarker", required = true)
    private String nextMarker;

    @XmlElement(name = "MaxKeys", required = true)
    private int maxKeys;

    @XmlElement(name = "IsTruncated", required = true)
    private boolean isTruncated;

    @XmlElement(name = "Delimiter", required = true)
    private String delimiter;

    @XmlElement(name = "StartAfter", required = true)
    private String startAfter;

    @XmlElement(name = "ContinuationToken", required = true)
    private String continuationToken;

    @XmlElement(name = "NextContinuationToken", required = true)
    private String nextContinuationToken;

    @XmlElement(name = "KeyCount", required = true)
    private String keyCount;

    @XmlTransient
    private boolean fetchOwner;

    @XmlTransient
    private String listType;

    @XmlElement(name = "Contents", required = true)
    private List<Contents> contents;

    @XmlElement(name = "CommonPrefixes", required = true)
    private List<Prefix> prefixlist;


}

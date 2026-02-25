package com.macrosan.message.xmlmsg.versions;

import com.macrosan.message.xmlmsg.section.Prefix;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ListVersionsResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ListVersionsResult", propOrder = {
        "name",
        "prefix",
        "keyMarker",
        "versionIdMarker",
        "nextKeyMarker",
        "nextVersionIdMarker",
        "maxKeys",
        "delimiter",
        "isTruncated",
        "version",
        "prefixlist"
})
public class ListVersionsResult {
    @XmlElement(name = "Name", required = true)
    private String name;

    @XmlElement(name = "Prefix", required = true)
    private String prefix;

    @XmlElement(name = "KeyMarker",required = true)
    private String keyMarker;

    @XmlElement(name = "VersionIdMarker", required = true)
    private String versionIdMarker;

    @XmlElement(name = "NextKeyMarker")
    private String nextKeyMarker;

    @XmlElement(name = "NextVersionIdMarker")
    private String nextVersionIdMarker;

    @XmlElement(name = "MaxKeys", required = true)
    private int maxKeys;

    @XmlElement(name = "Delimiter")
    private String delimiter;

    @XmlElement(name = "IsTruncated", required = true)
    private boolean isTruncated;

    @XmlElements({
            @XmlElement(name = "Version", type = Version.class, required = true),
            @XmlElement(name = "DeleteMarker", type = DeleteMarker.class, required = true)
    })
    private List<VersionBase> version;

    @XmlElement(name = "CommonPrefixes", required = true)
    private List<Prefix> prefixlist;

}

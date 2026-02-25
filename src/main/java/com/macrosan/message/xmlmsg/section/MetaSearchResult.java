package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "MetaSearchResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "MetaSearchResult", propOrder = {
        "UserId",
        "BucketName",
        "Key",
        "Size",
        "ETag",
        "LastModified",
        "metalists",
        "VersionId",
        "ContentType",
        "DeleteSource"
})
public class MetaSearchResult {

    @XmlElement(name = "UserId", required = true)
    private String UserId;
    @XmlElement(name = "BucketName", required = true)
    private String BucketName;
    @XmlElement(name = "Key", required = true)
    private String Key;
    @XmlElement(name = "Size", required = true)
    private String Size;
    @XmlElement(name = "ETag", required = true)
    private String ETag;
    @XmlElement(name = "LastModified", required = true)
    private String LastModified;
    @XmlElement(name = "Metas", required = true)
    private List<String> metalists;
    @XmlElement(name = "VersionId", required = true)
    private String VersionId;
    @XmlElement(name = "ContentType", required = true)
    private String ContentType;
    @XmlElement(name = "DeleteSource", required = true)
    private boolean DeleteSource;
}

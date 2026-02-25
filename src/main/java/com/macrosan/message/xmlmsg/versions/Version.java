package com.macrosan.message.xmlmsg.versions;

import com.macrosan.message.xmlmsg.section.Owner;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Created by zhanglinlin.
 *
 * @description
 * @date 2019/9/25 15:19
 */
@Data
@EqualsAndHashCode(callSuper = true)
@Accessors(chain = true)
@XmlRootElement(name = "Version")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Versions", propOrder = {
        "key",
        "versionId",
        "isLatest",
        "lastModified",
        "etag",
        "size",
        "owner",
        "storageClass"
})
public class Version extends VersionBase {

    @XmlElement(name = "Key", required = true)
    private String key;

    @XmlElement(name = "VersionId", required = true)
    private String versionId;

    @XmlElement(name = "IsLatest", required = true)
    private boolean isLatest;

    @XmlElement(name = "LastModified", required = true)
    private String lastModified;

    @XmlElement(name = "ETag", required = true)
    private String etag;

    @XmlElement(name = "Size", required = true)
    private String size;

    @XmlElement(name = "Owner", required = true)
    private Owner owner;

    @XmlElement(name = "StorageClass", required = true)
    private String storageClass;

}

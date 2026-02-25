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
 * @date 2019/9/25 15:20
 */
@Data
@Accessors(chain = true)
@EqualsAndHashCode(callSuper = true)
@XmlRootElement(name = "DeleteMarker")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "DeleteMarker", propOrder = {
        "key",
        "versionId",
        "isLatest",
        "lastModified",
        "storageClass",
        "owner"
})
public class DeleteMarker extends VersionBase {
    @XmlElement(name = "Key", required = true)
    private String key;

    @XmlElement(name = "VersionId", required = true)
    private String versionId;

    @XmlElement(name = "IsLatest", required = true)
    private boolean isLatest;

    @XmlElement(name = "LastModified", required = true)
    private String lastModified;

    @XmlElement(name = "Owner", required = true)
    private Owner owner;

    @XmlElement(name = "StorageClass", required = true)
    private String storageClass;

}

package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Delete
 * 保存批量删除成功对象列表的容器
 *
 * @author zhoupeng
 * @date 2019/7/26
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "Deleted")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(namespace = "Deleted", propOrder = {
        "key",
        "versionId",
        "deleteMarker",
        "deleteMarkerVersionId"
})
public class Deleted {
    @XmlElement(name = "Key", required = true)
    private String key;

    @XmlElement(name = "VersionId", required = false)
    private String versionId;

    @XmlElement(name = "DeleteMarker", required = false)
    private Boolean deleteMarker;

    @XmlElement(name = "DeleteMarkerVersionId", required = false)
    private String deleteMarkerVersionId;
}
package com.macrosan.message.xmlmsg.section;

/**
 * ObjectsList
 * 保存 Object 信息的容器
 *
 * @author zhoupeng
 * @date 2019/7/25
 */

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "Object")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Object", propOrder = {
        "key",
        "versionId",
})
public class ObjectsList {
    @XmlElement(name = "Key", required = true)
    private String key;

    @XmlElement(name = "VersionId", required = false)
    private String versionId;

}

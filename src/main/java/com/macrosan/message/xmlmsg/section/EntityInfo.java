package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * EntityInfo
 * 保存实体信息的容器
 *
 * @author shilinyong
 * @date 2019/08/12
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "EntityInfo")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "EntityInfo", propOrder = {
        "entityId",
        "entityName",
        "entityNickName",
        "entityType"
})
public class EntityInfo {

    @XmlElement(name = "EntityId", required = true)
    private String entityId;
    @XmlElement(name = "EntityName", required = true)
    private String entityName;
    @XmlElement(name = "EntityNickName", required = true)
    private String entityNickName;
    @XmlElement(name = "EntityType", required = true)
    private String entityType;
}

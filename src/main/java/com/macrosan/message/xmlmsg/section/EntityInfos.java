package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * EntityInfos
 * 保存用户信息的容器
 *
 * @author shilinyong
 * @date 2019/08/12
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "EntityInfos")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "EntityInfos", propOrder = {
        "entityInfo",
})
public class EntityInfos {

    @XmlElement(name = "EntityInfo", required = true)
    private List<EntityInfo> entityInfo;
}

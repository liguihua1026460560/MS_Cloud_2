package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.EntityInfos;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * EntityInfosResponse
 * 保存实体列表响应信息的容器
 *
 * @author shilinyong
 * @date 2019/08/12
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "EntityInfosResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "EntityInfosResponse", propOrder = {
        "entityInfos",
})
public class EntityInfosResponse {

    @XmlElement(name = "EntityInfos", required = true)
    private EntityInfos entityInfos;
}

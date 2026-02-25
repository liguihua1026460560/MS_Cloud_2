package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Owner
 * 保存 Bucket 拥有者信息的容器
 *
 * @author liyixin
 * @date 2018/11/12
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "Owner")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Owner", propOrder = {
        "id",
        "displayName"
})
public class Owner {

    @XmlElement(name = "ID", required = true)
    private String id;

    @XmlElement(name = "DisplayName", required = true)
    private String displayName;
}

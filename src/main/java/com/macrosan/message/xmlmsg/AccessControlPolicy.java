package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.Grant;
import com.macrosan.message.xmlmsg.section.Owner;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * AccessControlPolicy
 * 保存整个访问控制列表的容器
 *
 * @author liyixin
 * @date 2018/11/12
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "AccessControlPolicy")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AccessControlPolicy", propOrder = {
        "owner",
        "accessControlList"
})
public class AccessControlPolicy {

    @XmlElement(name = "Owner", required = true)
    private Owner owner;

    @XmlElement(name = "Grant", required = true)
    @XmlElementWrapper(name = "AccessControlList")
    private List<Grant> accessControlList;
}

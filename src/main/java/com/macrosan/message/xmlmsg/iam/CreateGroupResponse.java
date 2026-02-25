package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/7/31 19:11
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "CreateGroupResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "CreateGroupResponse", propOrder = {
        "createGroupResult",
        "responseMetadata"
})
public class CreateGroupResponse {
    @XmlElement(name = "CreateGroupResult", required = true)
    CreateGroupResult createGroupResult;

    @XmlElement(name = "ResponseMetadata",required = true)
    ResponseMetadata responseMetadata;

    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "CreateGroupResult")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "CreateGroupResult",propOrder = {
            "group"
    })

    public static class CreateGroupResult{
        @XmlElement(name = "Group",required = true)
        Group group;
    }
}

package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/8/3 17:47
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ListGroupsForUserResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ListGroupsForUserResponse",propOrder = {
        "listGroupsForUserResult",
        "responseMetadata"
})
public class ListGroupsForUserResponse {
    @XmlElement(name = "ListGroupsForUserResult",required = true)
    ListGroupsForUserResult listGroupsForUserResult;
    @XmlElement(name = "ResponseMetadata",required = true)
    ResponseMetadata responseMetadata;

    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "ListGroupsForUserResult")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "ListGroupsForUserResult",propOrder = {
            "groups",
            "isTruncated"
    })
    public static class ListGroupsForUserResult{
        @XmlElement(name = "Groups",required = true)
        Groups groups;
        @XmlElement(name = "IsTruncated",required = true)
        boolean isTruncated;
    }

    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "Groups")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "Groups",namespace = "listGroupsForUser",propOrder = {
            "member"
    })
    public static class Groups{
        @XmlElement(name = "member",required = true)
        List<GroupMember> member;
    }
}

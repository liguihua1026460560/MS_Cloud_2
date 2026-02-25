package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/7/31 19:31
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ListGroupsResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ListGroupsResponse", propOrder = {
        "listGroupsResult",
        "responseMetadata"
})
public class ListGroupsResponse {
    @XmlElement(name = "ListGroupsResult",required = true)
    ListGroupsResult listGroupsResult;
    @XmlElement(name = "ResponseMetadata",required = true)
    ResponseMetadata responseMetadata;

    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "ListGroupsResult")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "ListGroupsResult", propOrder = {
            "groups",
            "isTruncated"
    })
    public static class ListGroupsResult{
        @XmlElement(name = "Groups" ,required = true)
        Groups groups;
        @XmlElement(name = "IsTruncated", required = true)
        boolean isTruncated;
    }


    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "Groups")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "Groups",propOrder = {
            "members"
    })
    public static class Groups{
        @XmlElement(name = "member",required = true)
        List<GroupMember> members;
    }
}

package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/8/1 9:16
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "GetGroupResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "GetGroupResponse", propOrder = {
        "getGroupResult",
        "responseMetadata"
})
public class GetGroupResponse {
    @XmlElement(name = "GetGroupResult", required = true)
    GetGroupResult getGroupResult;
    @XmlElement(name = "ResponseMetadata", required = true)
    ResponseMetadata responseMetadata;

    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "GetGroupResult")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "GetGroupResult", propOrder = {
            "group",
            "users",
            "isTruncated"
    })
    public static class GetGroupResult {
        @XmlElement(name = "Group", required = true)
        Group group;
        @XmlElement(name = "Users", required = true)
        Users users;
        @XmlElement(name = "IsTruncated", required = true)
        boolean isTruncated;
    }

    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "Users")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "getGroup.Users", propOrder = {
            "member"
    })
    public static class Users {
        List<Member> member;
    }

    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "member")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "getGroup.member", propOrder = {
            "userId",
            "path",
            "userName",
            "arn",
            "createDate"
    })
    public static class Member {
        @XmlElement(name = "UserId", required = true)
        String userId;
        @XmlElement(name = "Path", required = true)
        String path;
        @XmlElement(name = "UserName", required = true)
        String userName;
        @XmlElement(name = "Arn", required = true)
        String arn;
        @XmlElement(name = "CreateDate", required = true)
        String createDate;
    }
}

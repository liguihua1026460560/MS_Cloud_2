package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ListUsersResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ListUsersResponse", propOrder = {
        "listUsersResult",
        "responseMetadata"
})
public class ListUsersResponse {
    @XmlElement(name = "ListUsersResult", required = true)
    ListUsersResult listUsersResult;
    @XmlElement(name = "ResponseMetadata", required = true)
    ResponseMetadata responseMetadata;

    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "ListUsersResult")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "ListUsersResult", propOrder = {
            "users",
            "isTruncated"
    })
    public static class ListUsersResult {
        @XmlElement(name = "Users", required = true)
        Users users;

        @XmlElement(name = "IsTruncated", required = true)
        boolean isTruncated;
    }

    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "Users")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "Users", propOrder = {
            "member"
    })
    public static class Users {
        List<Member> member;
    }

    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "member")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "user.member", propOrder = {
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



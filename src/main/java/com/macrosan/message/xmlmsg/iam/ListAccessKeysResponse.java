package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ListAccessKeysResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ListAccessKeysResponse", propOrder = {
        "listAccessKeysResult",
        "responseMetadata"
})
public class ListAccessKeysResponse {
    @XmlElement(name = "ListAccessKeysResult", required = true)
    ListAccessKeysResult listAccessKeysResult;
    @XmlElement(name = "ResponseMetadata", required = true)
    ResponseMetadata responseMetadata;

    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "ListAccessKeysResult")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "ListAccessKeysResult", propOrder = {
            "accessKeyMetadata",
            "isTruncated"
    })
    public static class ListAccessKeysResult {

        @XmlElement(name = "AccessKeyMetadata", required = true)
        AccessKeyMetadata accessKeyMetadata;

        @XmlElement(name = "IsTruncated", required = true)
        boolean isTruncated;
    }

    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "AccessKeyMetadata")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "AccessKeyMetadata", propOrder = {
            "member"
    })
    public static class AccessKeyMetadata {
        List<Member> member;
    }

    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "member")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "ak.member", propOrder = {
            "userName",
            "accessKeyId",
            "status",
            "createDate"
    })
    public static class Member {
        @XmlElement(name = "UserName", required = false)
        String userName;
        @XmlElement(name = "AccessKeyId", required = true)
        String accessKeyId;
        @XmlElement(name = "Status", required = true)
        String status;
        @XmlElement(name = "CreateDate", required = true)
        String createDate;
    }
}

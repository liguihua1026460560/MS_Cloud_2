package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/8/4 9:36
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ListAttachedUserPoliciesResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ListAttachedUserPoliciesResponse",propOrder = {
        "listUserPoliciesResult",
        "responseMetadata"
})
public class ListAttachedUserPoliciesResponse {
    @XmlElement(name = "ListAttachedUserPoliciesResult",required = true)
    ListAttachedUserPoliciesResult listUserPoliciesResult;
    @XmlElement(name = "ResponseMetadata",required = true)
    ResponseMetadata responseMetadata;


    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "ListAttachedUserPoliciesResult")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "ListAttachedUserPoliciesResult",propOrder = {
            "attachedPolicies",
            "isTruncated"
    })
    public static class ListAttachedUserPoliciesResult{
        @XmlElement(name = "IsTruncated",required = true)
        boolean isTruncated;
        @XmlElement(name = "AttachedPolicies",required = true)
        AttachedPolicies attachedPolicies;
    }


    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "AttachedPolicies")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "AttachedPolicies",propOrder = {
            "members"
    })
    public static class AttachedPolicies{
        @XmlElement(name = "member",required = true)
        List<StringMember> members;
    }

    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "member")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "member",namespace = "policies",propOrder = {
            "policyName",
            "policyArn"
    })
    public static class StringMember{
        @XmlElement(name = "PolicyName",required = true)
        String policyName;
        @XmlElement(name = "PolicyArn",required = true)
        String policyArn;
    }
}

package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/8/4 9:56
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ListAttachedGroupPoliciesResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ListAttachedGroupPoliciesResponse",propOrder = {
        "listAttachedGroupPoliciesResult",
        "responseMetadata"
})
public class ListAttachedGroupPoliciesResponse {
    @XmlElement(name = "ListAttachedGroupPoliciesResult",required = true)
    ListAttachedGroupPoliciesResult listAttachedGroupPoliciesResult;
    @XmlElement(name = "ResponseMetadata",required = true)
    ResponseMetadata responseMetadata;



    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "ListAttachedGroupPoliciesResult")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "ListAttachedGroupPoliciesResult",propOrder = {
            "attachedPolicies",
            "isTruncated"
    })
    public static class ListAttachedGroupPoliciesResult{
        @XmlElement(name = "AttachedPolicies",required = true)
        ListAttachedUserPoliciesResponse.AttachedPolicies attachedPolicies;
        @XmlElement(name = "IsTruncated",required = true)
        boolean isTruncated;
    }
}

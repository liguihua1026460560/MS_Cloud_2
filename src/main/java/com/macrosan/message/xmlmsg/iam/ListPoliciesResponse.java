package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/8/1 14:57
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ListPoliciesResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ListPoliciesResponse",propOrder = {
        "listPoliciesResult",
        "responseMetadata"
})
public class ListPoliciesResponse {
    @XmlElement(name = "ListPoliciesResult",required = true)
    ListPoliciesResult listPoliciesResult;
    @XmlElement(name = "ResponseMetadata",required = true)
    ResponseMetadata responseMetadata;



    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "ListPoliciesResult")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "ListPoliciesResult",propOrder = {
            "isTruncated",
            "marker",
            "policies"
    })
    public static class ListPoliciesResult{
        @XmlElement(name = "IsTruncated",required = true)
        boolean isTruncated;
        @XmlElement(name = "Marker",required = true)
        String marker;
        @XmlElement(name = "Policies",required = true)
        Policies policies;
    }


    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "Policies")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "Policies",propOrder = {
            "member"
    })
    public static class Policies{
        @XmlElement(name = "member",required = true)
        List<PolicyMember> member;
    }
}

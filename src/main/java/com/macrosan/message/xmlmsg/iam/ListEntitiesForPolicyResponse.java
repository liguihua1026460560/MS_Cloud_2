package com.macrosan.message.xmlmsg.iam;

import com.macrosan.message.xmlmsg.iam.EntitiesForPolicy.PolicyGroups;
import com.macrosan.message.xmlmsg.iam.EntitiesForPolicy.PolicyRoles;
import com.macrosan.message.xmlmsg.iam.EntitiesForPolicy.PolicyUsers;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/8/3 18:49
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ListEntitiesForPolicyResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ListEntitiesForPolicyResponse", propOrder = {
        "listEntitiesForPolicyResult",
        "responseMetadata"
})
public class ListEntitiesForPolicyResponse {
    @XmlElement(name = "ListEntitiesForPolicyResult", required = true)
    ListEntitiesForPolicyResult listEntitiesForPolicyResult;
    @XmlElement(name = "ResponseMetadata", required = true)
    ResponseMetadata responseMetadata;


    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "ListEntitiesForPolicyResult")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "ListEntitiesForPolicyResult", propOrder = {
            "policyRoles",
            "policyGroups",
            "policyUsers",
            "isTruncated"
    })
    public static class ListEntitiesForPolicyResult {
        @XmlElement(name = "PolicyRoles", required = true)
        PolicyRoles policyRoles = new PolicyRoles();
        @XmlElement(name = "PolicyGroups", required = true)
        PolicyGroups policyGroups = new PolicyGroups();
        @XmlElement(name = "PolicyUsers", required = true)
        PolicyUsers policyUsers = new PolicyUsers();
        @XmlElement(name = "IsTruncated", required = true)
        boolean isTruncated;
    }
}

package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/8/22 17:04
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "CreatePolicyVersionResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "CreatePolicyVersionResponse", propOrder = {
        "createPolicyVersionResult",
        "responseMetadata"
})
public class CreatePolicyVersionResponse {
    @XmlElement(name = "CreatePolicyVersionResult",required = true)
    CreatePolicyVersionResult createPolicyVersionResult;
    @XmlElement(name = "ResponseMetadata",required = true)
    ResponseMetadata responseMetadata;


    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "CreatePolicyVersionResult")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "CreatePolicyVersionResult", propOrder = {
            "policyVersion"
    })
    public static class CreatePolicyVersionResult{
        @XmlElement(name = "PolicyVersion",required = true)
        PolicyVersion policyVersion;
    }

    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "PolicyVersion")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "PolicyVersion",propOrder = {
            "isDefaultVersion",
            "versionId",
            "createDate"
    })
    public static class PolicyVersion{
        @XmlElement(name = "IsDefaultVersion",required = true)
        boolean isDefaultVersion;
        @XmlElement(name = "VersionId",required = true)
        String versionId;
        @XmlElement(name = "CreateDate",required = true)
        String createDate;
    }
}

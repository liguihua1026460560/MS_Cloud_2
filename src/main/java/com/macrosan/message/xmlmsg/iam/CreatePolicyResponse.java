package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/8/1 11:51
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "CreatePolicyResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "CreatePolicyResponse",propOrder = {
        "createPolicyResult",
        "responseMetadata"
})
public class CreatePolicyResponse {
    @XmlElement(name = "CreatePolicyResult",required = true)
    CreatePolicyResult createPolicyResult;
    @XmlElement(name = "ResponseMetadata",required = true)
    ResponseMetadata responseMetadata;


    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "CreatePolicyResult")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "CreatePolicyResult",propOrder = {
            "policy"
    })
    public static class CreatePolicyResult{
        @XmlElement(name = "Policy",required = true)
        Policy policy;
    }
}

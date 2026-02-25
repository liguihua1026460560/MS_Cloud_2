package com.macrosan.message.xmlmsg.iam;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Created by IntelliJ IDEA
 *
 * @Author :   joshua zhang
 * @Create :   2023/8/1 15:22
 * @Email :   joshua_zhang_email@163.com
 */

@Data
@Accessors(chain = true)
@XmlRootElement(name = "GetPolicyResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "GetPolicyResponse", propOrder = {
        "getPolicyResult",
        "responseMetadata"
})
public class GetPolicyResponse {
    @XmlElement(name = "GetPolicyResult", required = true)
    GetPolicyResult getPolicyResult;
    @XmlElement(name = "ResponseMetadata", required = true)
    ResponseMetadata responseMetadata;


    @Data
    @Accessors(chain = true)
    @XmlRootElement(name = "GetPolicyResult")
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "GetPolicyResult", propOrder = {
            "policy"
    })
    public static class GetPolicyResult {
        @XmlElement(name = "Policy", required = true)
        Policy policy;
    }

}

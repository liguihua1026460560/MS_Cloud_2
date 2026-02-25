package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * PolicyInfo
 * 保存策略信息的容器
 *
 * @author shilinyong
 * @date 2019/08/08
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "PolicyInfo")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "PolicyInfo", propOrder = {
        "policyId",
        "policyName",
        "policyType",
        "remark",
        "createTime",
        "policyMRN",
        "policyDocument"
})
public class PolicyInfo {

    @XmlElement(name = "PolicyId", required = true)
    private String policyId;
    @XmlElement(name = "PolicyName", required = true)
    private String policyName;
    @XmlElement(name = "PolicyType", required = true)
    private String policyType;
    @XmlElement(name = "Remark", required = true)
    private String remark;
    @XmlElement(name = "CreateTime", required = true)
    private String createTime;
    @XmlElement(name = "PolicyMRN", required = true)
    private String policyMRN;
    @XmlElement(name = "PolicyDocument", required = true)
    private String policyDocument;
}

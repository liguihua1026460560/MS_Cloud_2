package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * AuthInfo
 * 保存权限信息的容器
 *
 * @author shilinyong
 * @date 2019/08/13
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "AuthInfo")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AuthInfo", propOrder = {
        "entityName",
        "entityType",
        "policyName",
        "policyType",
        "policyRemark"
})
public class AuthInfo {

    @XmlElement(name = "EntityName", required = true)
    private String entityName;
    @XmlElement(name = "EntityType", required = true)
    private String entityType;
    @XmlElement(name = "PolicyName", required = true)
    private String policyName;
    @XmlElement(name = "PolicyType", required = true)
    private String policyType;
    @XmlElement(name = "PolicyRemark", required = true)
    private String policyRemark;
}

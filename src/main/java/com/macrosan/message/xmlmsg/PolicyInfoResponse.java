package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.PolicyInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * PolicyInfoResponse
 * 保存策略响应信息的容器
 *
 * @author shilinyong
 * @date 2019/08/08
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "PolicyInfoResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "PolicyInfoResponse", propOrder = {
        "policyInfo"
})
public class PolicyInfoResponse {

    @XmlElement(name = "PolicyInfo", required = true)
    private PolicyInfo policyInfo;
}

package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.PolicyInfos;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * PolicyInfosResponse
 * 保存用户列表响应信息的容器
 *
 * @author shilinyong
 * @date 2019/08/08
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "PolicyInfosResponse")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "PolicyInfosResponse", propOrder = {
        "policyInfos"
})
public class PolicyInfosResponse {

    @XmlElement(name = "PolicyInfos", required = true)
    private PolicyInfos policyInfos;
}

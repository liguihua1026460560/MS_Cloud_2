package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * PolicyInfos
 * 保存策略信息的容器
 *
 * @author shilinyong
 * @date 2019/07/19
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "PolicyInfos")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "PolicyInfos", propOrder = {
        "policyInfo",
})
public class PolicyInfos {

    @XmlElement(name = "PolicyInfo", required = true)
    private List<PolicyInfo> policyInfo;
}

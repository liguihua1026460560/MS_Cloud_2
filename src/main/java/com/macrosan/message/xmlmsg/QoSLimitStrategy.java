package com.macrosan.message.xmlmsg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * @Author: WANG CHENXING
 * @Date: 2022/12/1
 * @Description: 用于QoS相关接口的响应
 */

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "QosLimitStrategy")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "QosLimitStrategy", propOrder = {
        "strategyParts"      // 该处字母名称对应下方的变量名
})
public class QoSLimitStrategy {

    @XmlElement(name = "StrategyPart", required = true)
    private List<StrategyPart> strategyParts;

}

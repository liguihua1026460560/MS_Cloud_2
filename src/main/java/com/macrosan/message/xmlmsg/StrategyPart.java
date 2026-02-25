package com.macrosan.message.xmlmsg;

/**
 * @Author: WANG CHENXING
 * @Date: 2022/12/16
 * @Description:
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "StrategyPart")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "StrategyPart", propOrder = {
        "limitStrategy",
        "qosEnabledHour"
})
public class StrategyPart {

    @XmlElement(name = "LimitStrategy", required = true)
    private String limitStrategy;

    @XmlElement(name = "QoSEnabledHour", required = true) // 若发起请求设置其它策略生效，则必须设定生效的时间段
    private List<QoSEnabledHour> qosEnabledHour;

}

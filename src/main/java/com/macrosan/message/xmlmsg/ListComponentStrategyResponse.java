package com.macrosan.message.xmlmsg;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * @author zhaoyang
 * @date 2023/07/25
 **/

@Data
@Accessors(chain = true)
@XmlRootElement(name = "ComponentStrategies")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ComponentStrategies", propOrder = {
        "componentStrategyList",
})
public class ListComponentStrategyResponse {
    @XmlElementWrapper(name = "ComponentStrategyList")
    @XmlElement(name = "ComponentStrategy", required = true)
    List<ComponentStrategy> componentStrategyList;
}

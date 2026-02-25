package com.macrosan.message.xmlmsg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * 保存账户下流量统计信息的容器
 *
 * @author sunfang
 * @date 2019/7/11
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "TrafficStatisticsResultList")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "TrafficStatisticsResultList", propOrder = {
        "trafficStatisticsResultList"
})
public class TrafficStatisticsResultList {

    @XmlElement(name = "TrafficStatisticsResult", required = true)
    private List<TrafficStatisticsResult> trafficStatisticsResultList;

}


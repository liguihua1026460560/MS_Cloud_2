package com.macrosan.message.xmlmsg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * 保存账户性能配额的容器
 *
 * @author wuhaizhong
 * @date 2019/07/25
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "PerformanceQuota")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "PerformanceQuota", propOrder = {
        "clusterName",
        "throughPut",
        "bandWidth"
})

public class PerformanceQuota {

    @XmlElement(name = "ClusterName")
    private String clusterName;

    @XmlElement(name = "Throughput", required = true)
    private String throughPut;

    @XmlElement(name = "Bandwidth", required = true)
    private String bandWidth;

}

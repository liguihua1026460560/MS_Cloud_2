package com.macrosan.message.xmlmsg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * 保存账户下对象统计的容器
 *
 * @author sunfang
 * @date 2019/7/11
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "ObjectStatisticsResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "ObjectStatisticsResult", propOrder = {
        "objectCount",
        "storageCapacityUsed"
})
public class ObjectStatisticsResult {
    @XmlElement(name = "ObjectCount", required = true)
    private String objectCount;

    @XmlElement(name = "StorageCapacityUsed", required = true)
    private String storageCapacityUsed;

}

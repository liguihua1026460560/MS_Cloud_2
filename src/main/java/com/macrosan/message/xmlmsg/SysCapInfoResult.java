package com.macrosan.message.xmlmsg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * 保存MOSS容量信息的容器
 *
 * @author sunfang
 * @date 2019/7/1
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "SysCapInfoResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "SysCapInfoResult", propOrder = {
        "totalAvailableCapacity",
        "remainingAvailableCapacity",
        "usedAvailableCapacity",
        "totalCapacity",
        "availableCapacity",
        "usedCapacity"
})
public class SysCapInfoResult {

    @XmlElement(name = "TotalAvailableCapacity", required = true)
    private String totalAvailableCapacity;

    @XmlElement(name = "RemainingAvailableCapacity", required = true)
    private String remainingAvailableCapacity;

    @XmlElement(name = "UsedAvailableCapacity", required = true)
    private String usedAvailableCapacity;

    @XmlElement(name = "TotalCapacity", required = true)
    private String totalCapacity;

    @XmlElement(name = "AvailableCapacity", required = true)
    private String availableCapacity;

    @XmlElement(name = "UsedCapacity", required = true)
    private String usedCapacity;

}
package com.macrosan.message.xmlmsg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * 保存桶容量信息的容器
 *
 * @author wangyu
 * @date 2019/10/23
 * 包括可用容量和已使用容量
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "BucketCapInfoResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "BucketCapInfoResult", propOrder = {
        "usedCapacity",
        "availableCapacity"
})
public class BucketCapInfoResult {

    @XmlElement(name = "UsedCapacity", required = true)
    private String usedCapacity;

    @XmlElement(name = "AvailableCapacity", required = true)
    private String availableCapacity;
}

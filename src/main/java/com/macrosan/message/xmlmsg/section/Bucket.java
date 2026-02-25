package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * Bucket
 * 保存多个 Bucket 信息的容器
 *
 * @author liyixin
 * @date 2018/11/12
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "Bucket")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Bucket", propOrder = {
        "name",
        "creationDate",
        "storageStrategy",
        "capacityQuota",
        "softCapacityQuota",
        "usedCapacity",
        "usedCapacityRate",
        "objNumQuota",
        "softObjNumQuota",
        "usedObjNum",
        "usedObjNumRate"
})
public class Bucket {

    @XmlElement(name = "Name")
    private String name;

    @XmlElement(name = "CreationDate")
    private String creationDate;

    @XmlElement(name = "StorageStrategy")
    private String storageStrategy;

    @XmlElement(name = "CapacityQuota")
    private String capacityQuota;

    @XmlElement(name = "SoftCapacityQuota")
    private String softCapacityQuota;

    @XmlElement(name = "UsedCapacity")
    private String usedCapacity;

    @XmlElement(name = "UsedCapacityRate")
    private String usedCapacityRate;

    @XmlElement(name = "ObjNumQuota")
    private String objNumQuota;

    @XmlElement(name = "SoftObjNumQuota")
    private String softObjNumQuota;

    @XmlElement(name = "UsedObjNum")
    private String usedObjNum;

    @XmlElement(name = "UsedObjNumRate")
    private String usedObjNumRate;

}

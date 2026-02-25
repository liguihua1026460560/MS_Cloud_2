package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

/**
 * AccountInfo
 * 保存账户信息的容器
 *
 * @author shilinyong
 * @date 2020/03/12
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "AccountInfo")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AccountInfo", propOrder = {
        "accountId",
        "displayName",
        "accountName",
        "accountNickName",
        "remark",
        "createTime",
        "accountMRN",
        "capacityQuota",
        "softCapacityQuota",
        "usedCapacity",
        "leftCapacity",
        "usedRate",
        "throughPut",
        "bandWidth",
        "accessKey",
        "secretKey",
        "objectNumQuota",
        "softObjectNumQuota",
        "usedObjNum",
        "leftObjNum",
        "usedObjNumRate",
        "storageStrategy"
})
public class AccountInfo {

    @XmlElement(name = "AccountId", required = false)
    private String accountId;
    @XmlElement(name = "DisplayName", required = true)
    private String displayName;
    @XmlElement(name = "AccountName", required = true)
    private String accountName;
    @XmlElement(name = "AccountNickName", required = false)
    private String accountNickName;
    @XmlElement(name = "Remark", required = false)
    private String remark;
    @XmlElement(name = "CreateTime", required = false)
    private String createTime;
    @XmlElement(name = "AccountMRN", required = false)
    private String accountMRN;
    @XmlElement(name = "CapacityQuota", required = false)
    private String capacityQuota;
    @XmlElement(name = "SoftCapacityQuota", required = false)
    private String softCapacityQuota;
    @XmlElement(name = "UsedCapacity", required = false)
    private String usedCapacity;
    @XmlElement(name = "LeftCapacity", required = false)
    private String leftCapacity;
    @XmlElement(name = "UsedRate", required = false)
    private String usedRate;
    @XmlElement(name = "ThroughPut", required = false)
    private String throughPut;
    @XmlElement(name = "BandWidth", required = false)
    private String bandWidth;
    @XmlElement(name = "AccessKey", required = false)
    private String accessKey;
    @XmlElement(name = "SecretKey", required = true)
    private String secretKey;
    @XmlElement(name = "ObjectNumQuota", required = false)
    private String objectNumQuota;
    @XmlElement(name = "SoftObjectNumQuota", required = false)
    private String softObjectNumQuota;
    @XmlElement(name = "UsedObjNum", required = false)
    private String usedObjNum;
    @XmlElement(name = "LeftObjNum", required = false)
    private String leftObjNum;
    @XmlElement(name = "UsedObjNumRate", required = false)
    private String usedObjNumRate;
    @XmlElement(name = "StorageStrategy", required = false)
    private  String storageStrategy;
}

package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;


/**
 * @author sunfang
 * @date 2019/7/1
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "AccountUsedCapacity")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "AccountUsedCapacity", propOrder = {
        "id",
        "displayName",
        "usedCapacity",
        "objectCount"
})
public class AccountUsedCapacity {

    @XmlElement(name = "ID", required = true)
    private String id;

    @XmlElement(name = "DisplayName", required = true)
    private String displayName;

    @XmlElement(name = "UsedCapacity", required = true)
    private String usedCapacity;

    @XmlElement(name = "ObjectCount", required = true)
    private String objectCount;

}
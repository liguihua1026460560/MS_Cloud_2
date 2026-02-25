package com.macrosan.message.xmlmsg;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "FsUsedInfo")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "FsUsedInfo", propOrder = {
        "usedCapacity",
        "usedObjects"
})
public class FsUsedInfo {
    @XmlElement(name = "UsedCapacity", required = true)
    private String usedCapacity;
    @XmlElement(name = "UsedObjects", required = true)
    private String usedObjects;
}

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
@XmlRootElement(name = "FsCapInfoResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "FsCapInfoResult", propOrder = {
        "usedCapacity"
})
public class FsCapInfoResult {

    @XmlElement(name = "UsedCapacity", required = true)
    private String usedCapacity;
}

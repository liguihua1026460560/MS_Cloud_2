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
@XmlRootElement(name = "FsUsedObjectResult")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "FsUsedObjectResult", propOrder = {
        "usedObjects",
})
public class FsUsedObjectResult {
    @XmlElement(name = "UsedObjects", required = true)
    private String usedObjects;
}

package com.macrosan.message.xmlmsg.section;

import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "Objects")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "Objects", propOrder = {
        "name",
        "hash",
        "bytes",
        "last_modified"
})
public class ObjectsSwift {

    @XmlElement(name = "name", required = true)
    private String name;

    @XmlElement(name = "hash", required = true)
    private String hash;

    @XmlElement(name = "bytes", required = true)
    private String bytes;

    @XmlElement(name = "last_modified", required = true)
    private String last_modified;
}

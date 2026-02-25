package com.macrosan.message.xmlmsg;

import com.macrosan.message.xmlmsg.section.ObjectsSwift;
import lombok.Data;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

@Data
@Accessors(chain = true)
@XmlRootElement(name = "container")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "container", propOrder = {
        "objectList"
})
public class ListBucketSwift {

    @XmlElement(name = "object", required = true)
    private List<ObjectsSwift> objectList;
}

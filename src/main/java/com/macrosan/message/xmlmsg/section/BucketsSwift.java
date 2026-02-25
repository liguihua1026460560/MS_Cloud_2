package com.macrosan.message.xmlmsg.section;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;
import java.util.List;

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "account")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "account", propOrder = {
        "bucketList"
})
public class BucketsSwift {

    @XmlElement(name = "container", required = true)
    private List<BucketSwift> bucketList;
}

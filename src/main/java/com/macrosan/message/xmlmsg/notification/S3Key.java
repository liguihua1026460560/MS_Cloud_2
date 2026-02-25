package com.macrosan.message.xmlmsg.notification;


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
@XmlRootElement(name = "S3Key")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "S3Key",propOrder = {
        "filterRule",
})
public class S3Key {
    @XmlElement(name = "FilterRule")
    private List<FilterRule> filterRule;
}

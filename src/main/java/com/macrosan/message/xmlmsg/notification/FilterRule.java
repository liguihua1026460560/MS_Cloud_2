package com.macrosan.message.xmlmsg.notification;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.xml.bind.annotation.*;

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "FilterRule")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "FilterRule",propOrder = {
        "name",
        "value"
})
public class FilterRule {
    @XmlElement(name = "Name")
    private String name;
    @XmlElement(name = "Value")
    private String value;
}
